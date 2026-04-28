"""
Processador principal — orquestra: ERP → exportar por fluxo → Google Sheets.
Login 1 vez por ERP, exportar N vezes (1 por fluxo ativo, cada um com filtros diferentes no ERP).
Inclui retry de login, lock por automação e alerta de falha via WhatsApp.
"""

import logging
import time
from datetime import timedelta

from app.tz import agora, hoje

import pandas as pd

from app.models import Automacao, ERPConfig, Fluxo, Execucao
from app.services.erp_factory import criar_erp_client
from app.services.notifier import notify_failure
from app.services.sheets import SheetsWriter
from app.crypto import decrypt_password

logger = logging.getLogger(__name__)

ERP_LOGIN_MAX_RETRIES = 3
ERP_LOGIN_RETRY_INTERVAL = 30  # segundos



def _data_fim_dias_uteis(data_inicio, n: int):
    """Retorna a data do n-ésimo dia útil (seg-sex) a partir de data_inicio (inclusive)."""
    count = 0
    current = data_inicio
    while True:
        if current.weekday() < 5:
            count += 1
            if count == n:
                return current
        current += timedelta(days=1)


def processar_automacao(automacao: Automacao, db, agendado: bool = False, on_fluxo_start=None) -> dict:
    """
    Executa o passo a passo completo de uma automação:
    Para cada ERP configurado (Brasil, Truck ou ambos):
      1. Login no ERP (com retry)
      2. Para cada fluxo ativo, exportar com filtros próprios e escrever na aba
    """
    log_parts = []
    overall_status = "sucesso"
    total_encontrados = 0
    total_filtrados = 0

    try:
        # Extrair todos os dados necessários ANTES de qualquer commit
        # Isso evita problemas com objetos detached/expired após db.commit()
        automacao_id = automacao.id
        automacao_nome = automacao.nome
        sheets_url = automacao.sheets_url
        mapeamento = automacao.mapeamento or automacao.MAPEAMENTO_PADRAO

        # Copiar dados dos ERPs e fluxos para dicts simples
        erps_data = []
        for erp in automacao.erp_configs:
            if not erp.ativo:
                continue
            erp_dict = {
                "id": erp.id,
                "erp_tipo": erp.erp_tipo,
                "erp_url": erp.erp_url,
                "erp_login": erp.erp_login,
                "erp_senha_encrypted": erp.erp_senha,
                "fluxos": [],
            }
            for fluxo in erp.fluxos:
                if not fluxo.ativo:
                    continue
                erp_dict["fluxos"].append({
                    "id": fluxo.id,
                    "tipo": fluxo.tipo,
                    "nome": fluxo.nome,
                    "sheets_aba": fluxo.sheets_aba,
                    "formulario_id": fluxo.formulario_id,
                    "situacao_id": fluxo.situacao_id,
                    "filtro_dias_min": fluxo.filtro_dias_min,
                    "filtro_dias_max": fluxo.filtro_dias_max,
                })
            erps_data.append(erp_dict)

        log_parts.append(f"[{agora().isoformat()}] Iniciando automação: {automacao_nome}")

        writer = SheetsWriter()

        if not erps_data:
            log_parts.append("Nenhum ERP ativo. Nada a processar.")
            return {
                "status": "sucesso",
                "registros_encontrados": 0,
                "registros_filtrados": 0,
                "log": "\n".join(log_parts),
            }

        for erp_idx, erp_data in enumerate(erps_data):
            log_parts.append(f"\n{'='*40}")
            log_parts.append(f"ERP: {erp_data['erp_tipo']} ({erp_data['erp_url']})")
            log_parts.append(f"{'='*40}")

            # 1. Criar client ERP e fazer login com retry
            if on_fluxo_start:
                on_fluxo_start(automacao_id, erp_data["erp_tipo"], "Login...")
            senha = decrypt_password(erp_data["erp_senha_encrypted"])

            # Criar ERPConfig mínimo para o factory
            class _MinimalERPConfig:
                pass
            minimal_config = _MinimalERPConfig()
            minimal_config.erp_tipo = erp_data["erp_tipo"]
            minimal_config.erp_url = erp_data["erp_url"]
            minimal_config.erp_login = erp_data["erp_login"]

            client = criar_erp_client(minimal_config, senha)

            login_ok = False
            last_error = ""
            for attempt in range(1, ERP_LOGIN_MAX_RETRIES + 1):
                log_parts.append(f"Tentativa de login {attempt}/{ERP_LOGIN_MAX_RETRIES}...")
                try:
                    if client.login():
                        login_ok = True
                        log_parts.append(f"Login OK no ERP: {erp_data['erp_url']}")
                        break
                    else:
                        last_error = "Login retornou False — credenciais inválidas"
                        log_parts.append(f"Falha: {last_error}")
                except Exception as e:
                    last_error = str(e)
                    log_parts.append(f"Exceção no login: {last_error}")

                if attempt < ERP_LOGIN_MAX_RETRIES:
                    log_parts.append(f"Aguardando {ERP_LOGIN_RETRY_INTERVAL}s antes de tentar novamente...")
                    time.sleep(ERP_LOGIN_RETRY_INTERVAL)
                    client.close()
                    client = criar_erp_client(minimal_config, senha)

            if not login_ok:
                error_msg = f"Falha no login após {ERP_LOGIN_MAX_RETRIES} tentativas ({erp_data['erp_tipo']}): {last_error}"
                log_parts.append(f"ERRO: {error_msg}")
                notify_failure(automacao_nome, error_msg)
                for fluxo_data in erp_data["fluxos"]:
                    execucao = Execucao(
                        automacao_id=automacao_id,
                        erp_config_id=erp_data["id"],
                        fluxo_id=fluxo_data["id"],
                        status="erro",
                        registros_encontrados=0,
                        registros_filtrados=0,
                        log=error_msg,
                    )
                    db.add(execucao)
                db.commit()
                overall_status = "parcial"
                continue

            # 2. Exportar e escrever por fluxo
            if not erp_data["fluxos"]:
                log_parts.append(f"Nenhum fluxo ativo no ERP {erp_data['erp_tipo']}.")
                client.close()
                continue

            for i, fluxo_data in enumerate(erp_data["fluxos"]):
                # Notificar qual fluxo está rodando
                if on_fluxo_start:
                    on_fluxo_start(automacao_id, erp_data["erp_tipo"], fluxo_data["nome"])

                # Quando agendado, esperar 2 min entre fluxos
                if agendado and (erp_idx > 0 or i > 0):
                    log_parts.append(f"Aguardando 120s antes do próximo fluxo (execução agendada)...")
                    time.sleep(120)

                log_parts.append(f"\n--- Fluxo: {fluxo_data['nome']} ({fluxo_data['tipo']}) [{erp_data['erp_tipo']}] ---")

                # Calcular intervalo de datas para filtro client-side por vencimento_Parcela
                hoje_dt = hoje().replace(tzinfo=None)
                if fluxo_data["tipo"] == "preboleto":
                    amanha = hoje_dt + timedelta(days=1)
                    data_min = amanha
                    data_max = _data_fim_dias_uteis(amanha, 7)
                else:
                    data_min = hoje_dt + timedelta(days=fluxo_data["filtro_dias_min"])
                    data_max = hoje_dt + timedelta(days=fluxo_data["filtro_dias_max"])

                log_parts.append(f"Filtro: formulário={fluxo_data['formulario_id']}, situação={fluxo_data['situacao_id'] or 'N/A'}, período={data_min.strftime('%d/%m/%Y')} a {data_max.strftime('%d/%m/%Y')}")

                # Exportar sem filtro de data — ERP não filtra; aplicamos client-side
                try:
                    # Form 127000008 (pré-boleto) precisa de datas e tem coluna "vencimento"
                    if fluxo_data["formulario_id"] == "127000008":
                        resultado = client.exportar_form_008(data_min, data_max)
                    else:
                        resultado = client.exportar_inadimplencia(
                            id_formulario=fluxo_data["formulario_id"],
                            id_situacao=fluxo_data["situacao_id"],
                            dt_inicial="",
                            dt_final="",
                        )
                    df = resultado.dataframe
                    registros_bruto = len(df)
                    log_parts.append(f"Registros exportados (bruto): {registros_bruto}")
                    log_parts.append(f"Colunas ERP: {list(df.columns)}")

                    # Detectar coluna de vencimento (form 127000007=vencimento_Parcela, form 127000008=vencimento)
                    col_venc = next((c for c in ["vencimento_Parcela", "vencimento"] if c in df.columns), None)
                    if col_venc and registros_bruto > 0:
                        df[col_venc] = pd.to_datetime(df[col_venc], errors="coerce")
                        if fluxo_data["tipo"] == "vencendo_hoje":
                            # Vencimento no Dia: filtra pela data completa de hoje
                            df = df[df[col_venc].dt.normalize() == pd.Timestamp(hoje_dt.date())]
                        else:
                            df = df[
                                (df[col_venc].dt.normalize() >= pd.Timestamp(data_min.date())) &
                                (df[col_venc].dt.normalize() <= pd.Timestamp(data_max.date()))
                            ]

                    registros_encontrados = len(df)
                    total_encontrados += registros_encontrados
                    log_parts.append(f"Registros após filtro: {registros_encontrados}")
                except Exception as e:
                    log_parts.append(f"Erro ao exportar fluxo {fluxo_data['nome']}: {e}")
                    overall_status = "parcial"
                    notify_failure(automacao_nome, f"Erro ao exportar fluxo {fluxo_data['nome']} [{erp_data['erp_tipo']}]: {e}")
                    execucao = Execucao(
                        automacao_id=automacao_id,
                        erp_config_id=erp_data["id"],
                        fluxo_id=fluxo_data["id"],
                        status="erro",
                        registros_encontrados=0,
                        registros_filtrados=0,
                        log=f"Erro ao exportar: {e}",
                    )
                    db.add(execucao)
                    db.commit()
                    continue

                if registros_encontrados == 0:
                    if registros_bruto == 0:
                        detalhe_vazio = f"ERP retornou 0 registros"
                    else:
                        detalhe_vazio = f"ERP retornou {registros_bruto}, nenhum no período {data_min.strftime('%d/%m')}–{data_max.strftime('%d/%m')}"
                    log_parts.append(f"Nenhum registro para o fluxo {fluxo_data['nome']}. Limpando aba. ({detalhe_vazio})")
                    writer.write_data(
                        sheets_url=sheets_url,
                        aba=fluxo_data["sheets_aba"],
                        data=[],
                        mapeamento=mapeamento,
                    )
                    execucao = Execucao(
                        automacao_id=automacao_id,
                        erp_config_id=erp_data["id"],
                        fluxo_id=fluxo_data["id"],
                        status="vazio",
                        registros_encontrados=0,
                        registros_filtrados=0,
                        log=f"Fluxo {fluxo_data['nome']} [{erp_data['erp_tipo']}]: {detalhe_vazio}, aba limpa",
                    )
                    db.add(execucao)
                    db.commit()
                    continue

                total_filtrados += registros_encontrados

                # Converter para lista de dicts com limpeza de tipos
                registros = df.to_dict(orient="records")
                for registro in registros:
                    for key, value in registro.items():
                        if isinstance(value, pd.Timestamp):
                            registro[key] = value.strftime("%d/%m/%Y")
                        elif pd.isna(value):
                            registro[key] = ""
                        elif isinstance(value, int):
                            registro[key] = str(value)
                        elif isinstance(value, float):
                            if value == int(value):
                                registro[key] = str(int(value))
                            else:
                                registro[key] = str(value)

                # Escrever no Google Sheets
                log_parts.append(f"Escrevendo {len(registros)} registros na aba '{fluxo_data['sheets_aba']}'...")
                resultado_sheets = writer.write_data(
                    sheets_url=sheets_url,
                    aba=fluxo_data["sheets_aba"],
                    data=registros,
                    mapeamento=mapeamento,
                )

                log_parts.append(f"Resultado: {resultado_sheets['log']}")

                status_fluxo = "sucesso"
                if resultado_sheets["status"] == "erro":
                    status_fluxo = "erro"
                    overall_status = "parcial"
                    notify_failure(automacao_nome, f"Erro no fluxo {fluxo_data['nome']} [{erp_data['erp_tipo']}]: {resultado_sheets['log']}")
                elif resultado_sheets["linhas_escritas"] < len(registros):
                    status_fluxo = "parcial"
                    overall_status = "parcial"

                # Criar execução para este fluxo
                execucao = Execucao(
                    automacao_id=automacao_id,
                    erp_config_id=erp_data["id"],
                    fluxo_id=fluxo_data["id"],
                    status=status_fluxo,
                    registros_encontrados=registros_encontrados,
                    registros_filtrados=registros_encontrados,
                    log=f"Fluxo {fluxo_data['nome']} [{erp_data['erp_tipo']}]: {resultado_sheets['log']}",
                )
                db.add(execucao)
                db.commit()

            client.close()

        return {
            "status": overall_status,
            "registros_encontrados": total_encontrados,
            "registros_filtrados": total_filtrados,
            "log": "\n".join(log_parts),
        }

    except Exception as e:
        logger.exception(f"Erro ao processar automação")
        error_msg = str(e)
        log_parts.append(f"ERRO: {error_msg}")
        notify_failure(f"Automação", error_msg)
        return {
            "status": "erro",
            "registros_encontrados": 0,
            "registros_filtrados": 0,
            "log": "\n".join(log_parts),
        }


def processar_automacao_fluxo_unico(automacao: Automacao, fluxo_id: int, db, on_fluxo_start=None) -> dict:
    """
    Executa apenas um fluxo específico (pelo ID) — usado para execuções manuais de debug.
    Faz login no ERP do fluxo e processa somente aquele fluxo.
    """
    for erp in automacao.erp_configs:
        if not erp.ativo:
            continue
        for fluxo in erp.fluxos:
            if fluxo.id == fluxo_id:
                automacao_id = automacao.id
                automacao_nome = automacao.nome
                sheets_url = automacao.sheets_url
                mapeamento = automacao.mapeamento or automacao.MAPEAMENTO_PADRAO

                erp_data = {
                    "id": erp.id,
                    "erp_tipo": erp.erp_tipo,
                    "erp_url": erp.erp_url,
                    "erp_login": erp.erp_login,
                    "erp_senha_encrypted": erp.erp_senha,
                    "fluxos": [{
                        "id": fluxo.id,
                        "tipo": fluxo.tipo,
                        "nome": fluxo.nome,
                        "sheets_aba": fluxo.sheets_aba,
                        "formulario_id": fluxo.formulario_id,
                        "situacao_id": fluxo.situacao_id,
                        "filtro_dias_min": fluxo.filtro_dias_min,
                        "filtro_dias_max": fluxo.filtro_dias_max,
                    }],
                }

                automacao_mock = type("A", (), {
                    "id": automacao_id,
                    "nome": automacao_nome,
                    "sheets_url": sheets_url,
                    "mapeamento": mapeamento,
                    "MAPEAMENTO_PADRAO": automacao.MAPEAMENTO_PADRAO,
                    "erp_configs": [type("E", (), {"ativo": True, "id": erp.id,
                        "erp_tipo": erp.erp_tipo, "erp_url": erp.erp_url,
                        "erp_login": erp.erp_login, "erp_senha": erp.erp_senha,
                        "fluxos": [fluxo]})()],
                })()

                return processar_automacao(automacao_mock, db, agendado=False, on_fluxo_start=on_fluxo_start)

    return {
        "status": "erro",
        "registros_encontrados": 0,
        "registros_filtrados": 0,
        "log": f"Fluxo ID={fluxo_id} não encontrado na automação.",
    }