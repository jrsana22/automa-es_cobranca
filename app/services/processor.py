"""
Processador principal — orquestra: ERP → exportar por fluxo → Google Sheets.
Login 1 vez, exportar N vezes (1 por fluxo ativo, cada um com filtros diferentes no ERP).
Inclui retry de login, lock por automação e alerta de falha via WhatsApp.
"""

import logging
import time
from datetime import timedelta

from app.tz import agora, hoje

import pandas as pd

from app.models import Automacao, Fluxo, Execucao
from app.services.erp_factory import criar_erp_client
from app.services.notifier import notify_failure
from app.services.sheets import SheetsWriter
from app.crypto import decrypt_password

logger = logging.getLogger(__name__)

ERP_LOGIN_MAX_RETRIES = 3
ERP_LOGIN_RETRY_INTERVAL = 30  # segundos


def _calcular_datas(fluxo: Fluxo) -> tuple[str, str]:
    """
    Calcula dt_inicial e dt_final para o filtro do ERP baseado no fluxo.
    Retorna strings no formato DD/MM/AAAA.
    """
    hoje_dt = hoje().replace(tzinfo=None)
    data_min = hoje_dt + timedelta(days=fluxo.filtro_dias_min)
    data_max = hoje_dt + timedelta(days=fluxo.filtro_dias_max)
    return data_min.strftime("%d/%m/%Y"), data_max.strftime("%d/%m/%Y")


def processar_automacao(automacao: Automacao, db, agendado: bool = False) -> dict:
    """
    Executa o passo a passo completo de uma automação:
    1. Login no ERP (com retry)
    2. Para cada fluxo ativo, exportar com filtros próprios e escrever na aba
    """
    log_parts = []
    overall_status = "sucesso"
    client = None
    total_encontrados = 0
    total_filtrados = 0

    try:
        # 1. Criar client ERP e fazer login com retry
        log_parts.append(f"[{agora().isoformat()}] Iniciando automação: {automacao.nome}")
        senha = decrypt_password(automacao.erp_senha)
        client = criar_erp_client(automacao, senha)

        login_ok = False
        last_error = ""
        for attempt in range(1, ERP_LOGIN_MAX_RETRIES + 1):
            log_parts.append(f"Tentativa de login {attempt}/{ERP_LOGIN_MAX_RETRIES}...")
            try:
                if client.login():
                    login_ok = True
                    log_parts.append(f"Login OK no ERP: {automacao.erp_url}")
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
                client = criar_erp_client(automacao, senha)

        if not login_ok:
            error_msg = f"Falha no login após {ERP_LOGIN_MAX_RETRIES} tentativas: {last_error}"
            log_parts.append(f"ERRO: {error_msg}")
            notify_failure(automacao.nome, error_msg)
            return {
                "status": "erro",
                "registros_encontrados": 0,
                "registros_filtrados": 0,
                "log": "\n".join(log_parts),
            }

        # 2. Exportar e escrever por fluxo (cada um com seus próprios filtros no ERP)
        fluxos_ativos = [f for f in automacao.fluxos if f.ativo]
        if not fluxos_ativos:
            log_parts.append("Nenhum fluxo ativo. Nada a processar.")
            return {
                "status": "sucesso",
                "registros_encontrados": 0,
                "registros_filtrados": 0,
                "log": "\n".join(log_parts),
            }

        writer = SheetsWriter()
        mapeamento = automacao.mapeamento or automacao.MAPEAMENTO_PADRAO

        for i, fluxo in enumerate(fluxos_ativos):
            # Quando agendado, esperar 2 min entre fluxos (exceto antes do primeiro)
            if agendado and i > 0:
                log_parts.append(f"Aguardando 120s antes do próximo fluxo (execução agendada)...")
                time.sleep(120)

            log_parts.append(f"\n--- Fluxo: {fluxo.nome} ({fluxo.tipo}) ---")

            # Calcular datas do filtro
            dt_inicial, dt_final = _calcular_datas(fluxo)
            log_parts.append(f"Filtro: formulário={fluxo.formulario_id}, situação={fluxo.situacao_id or 'N/A'}, período={dt_inicial} a {dt_final}")

            # Exportar do ERP com filtros deste fluxo
            try:
                resultado = client.exportar_inadimplencia(
                    id_formulario=fluxo.formulario_id,
                    id_situacao=fluxo.situacao_id,
                    dt_inicial=dt_inicial,
                    dt_final=dt_final,
                )
                df = resultado.dataframe
                registros_encontrados = len(df)
                total_encontrados += registros_encontrados
                log_parts.append(f"Registros exportados: {registros_encontrados}")
            except Exception as e:
                log_parts.append(f"Erro ao exportar fluxo {fluxo.nome}: {e}")
                overall_status = "parcial"
                notify_failure(automacao.nome, f"Erro ao exportar fluxo {fluxo.nome}: {e}")
                # Criar execução com erro
                execucao = Execucao(
                    automacao_id=automacao.id,
                    fluxo_id=fluxo.id,
                    status="erro",
                    registros_encontrados=0,
                    registros_filtrados=0,
                    log=f"Erro ao exportar: {e}",
                )
                db.add(execucao)
                db.commit()
                continue

            if registros_encontrados == 0:
                log_parts.append(f"Nenhum registro para o fluxo {fluxo.nome}. Planilha não atualizada.")
                execucao = Execucao(
                    automacao_id=automacao.id,
                    fluxo_id=fluxo.id,
                    status="vazio",
                    registros_encontrados=0,
                    registros_filtrados=0,
                    log=f"Fluxo {fluxo.nome}: 0 registros exportados",
                )
                db.add(execucao)
                db.commit()
                continue

            total_filtrados += registros_encontrados

            # Converter para lista de dicts
            registros = df.to_dict(orient="records")
            for registro in registros:
                for key, value in registro.items():
                    if isinstance(value, pd.Timestamp):
                        registro[key] = value.strftime("%d/%m/%Y")
                    elif pd.isna(value):
                        registro[key] = ""
                    elif isinstance(value, float):
                        registro[key] = str(value)

            # Escrever no Google Sheets
            log_parts.append(f"Escrevendo {len(registros)} registros na aba '{fluxo.sheets_aba}'...")
            resultado_sheets = writer.write_data(
                sheets_url=automacao.sheets_url,
                aba=fluxo.sheets_aba,
                data=registros,
                mapeamento=mapeamento,
            )

            log_parts.append(f"Resultado: {resultado_sheets['log']}")

            status_fluxo = "sucesso"
            if resultado_sheets["status"] == "erro":
                status_fluxo = "erro"
                overall_status = "parcial"
                notify_failure(automacao.nome, f"Erro no fluxo {fluxo.nome}: {resultado_sheets['log']}")
            elif resultado_sheets["linhas_escritas"] < len(registros):
                status_fluxo = "parcial"
                overall_status = "parcial"

            # Criar execução para este fluxo
            execucao = Execucao(
                automacao_id=automacao.id,
                fluxo_id=fluxo.id,
                status=status_fluxo,
                registros_encontrados=registros_encontrados,
                registros_filtrados=registros_encontrados,
                log=f"Fluxo {fluxo.nome}: {resultado_sheets['log']}",
            )
            db.add(execucao)
            db.commit()

        return {
            "status": overall_status,
            "registros_encontrados": total_encontrados,
            "registros_filtrados": total_filtrados,
            "log": "\n".join(log_parts),
        }

    except Exception as e:
        logger.exception(f"Erro ao processar automação {automacao.nome}")
        error_msg = str(e)
        log_parts.append(f"ERRO: {error_msg}")
        notify_failure(automacao.nome, error_msg)
        return {
            "status": "erro",
            "registros_encontrados": 0,
            "registros_filtrados": 0,
            "log": "\n".join(log_parts),
        }

    finally:
        try:
            if client:
                client.close()
        except Exception:
            pass