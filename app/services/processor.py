"""
Processador principal — orquestra: ERP → filtrar por fluxos → Google Sheets.
Login 1 vez, exportar 1 vez, filtrar N vezes (1 por fluxo ativo).
Inclui retry de login, lock por automação e alerta de falha via WhatsApp.
"""

import logging
import time
from datetime import datetime, timedelta

from app.tz import agora, hoje

import pandas as pd

from app.models import Automacao, Fluxo, Execucao, FLUXO_COBRANCA_2_30
from app.services.erp_factory import criar_erp_client
from app.services.notifier import notify_failure
from app.services.sheets import SheetsWriter
from app.crypto import decrypt_password

logger = logging.getLogger(__name__)

ERP_LOGIN_MAX_RETRIES = 3
ERP_LOGIN_RETRY_INTERVAL = 30  # segundos


def _verificar_dia_cobranca(automacao: Automacao) -> bool:
    """
    Verifica se hoje é dia de executar o fluxo cobrança_2_30.
    O ciclo é de 3 dias: executa nos dias (dia_base, dia_base+3, dia_base+6, ...)
    """
    dia_base = automacao.dia_cobranca_base or 1
    dia_hoje = agora().day
    # Ajusta para que o ciclo comece no dia_base
    return (dia_hoje - dia_base) % 3 == 0 if dia_hoje >= dia_base else False


def _filtrar_por_fluxo(df: pd.DataFrame, fluxo: Fluxo, col_venc: str, automacao: Automacao) -> pd.DataFrame:
    """
    Filtra o DataFrame pelo intervalo de dias do fluxo.
    filtro_dias_min/max negativos = vencimento no futuro (pré-boleto)
    filtro_dias_min/max = 0 = vence hoje
    filtro_dias_min/max positivos = vencimento no passado (cobrança/reativação)
    """
    if col_venc not in df.columns:
        # Busca case-insensitive
        col_match = [c for c in df.columns if c.lower().strip() == col_venc.lower().strip()]
        if col_match:
            col_venc = col_match[0]
        else:
            raise ValueError(f"Coluna '{col_venc}' não encontrada. Colunas: {list(df.columns)}")

    # Converte coluna de vencimento para datetime
    df_copy = df.copy()
    df_copy["_vencimento_dt"] = pd.to_datetime(df_copy[col_venc], errors="coerce", dayfirst=True)

    hoje_dt = hoje()
    data_min = hoje_dt + timedelta(days=fluxo.filtro_dias_min)
    data_max = hoje_dt + timedelta(days=fluxo.filtro_dias_max)

    # Filtro: vencimento entre data_min e data_max (inclusive)
    df_filtrado = df_copy[
        (df_copy["_vencimento_dt"] >= data_min) &
        (df_copy["_vencimento_dt"] <= data_max)
    ]
    df_filtrado = df_filtrado.drop(columns=["_vencimento_dt"])

    return df_filtrado


def processar_automacao(automacao: Automacao, db, agendado: bool = False) -> dict:
    """
    Executa o passo a passo completo de uma automação:
    1. Login no ERP (com retry)
    2. Exportar relatório de inadimplência (1 vez)
    3. Para cada fluxo ativo, filtrar e escrever na aba correspondente
    """
    log_parts = []
    overall_status = "sucesso"
    client = None

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

        # 2. Exportar relatório (1 vez para todos os fluxos)
        log_parts.append("Exportando relatório de inadimplência...")
        resultado = client.exportar_inadimplencia()
        df = resultado.dataframe
        total_registros = len(df)
        log_parts.append(f"Registros encontrados: {total_registros}")
        log_parts.append(f"Colunas: {list(df.columns)}")

        # 3. Filtrar e escrever por fluxo
        fluxos_ativos = [f for f in automacao.fluxos if f.ativo]
        if not fluxos_ativos:
            log_parts.append("Nenhum fluxo ativo. Nada a processar.")
            return {
                "status": "sucesso",
                "registros_encontrados": total_registros,
                "registros_filtrados": 0,
                "log": "\n".join(log_parts),
            }

        col_venc = automacao.coluna_vencimento
        writer = SheetsWriter()
        mapeamento = automacao.mapeamento or automacao.MAPEAMENTO_PADRAO
        total_filtrados = 0

        # Verificar se hoje é dia de cobrança 2-30D (se houver esse fluxo)
        dia_cobranca = _verificar_dia_cobranca(automacao)

        for i, fluxo in enumerate(fluxos_ativos):
            # Quando agendado, esperar 2 min entre fluxos (exceto antes do primeiro)
            if agendado and i > 0:
                log_parts.append(f"Aguardando 120s antes do próximo fluxo (execução agendada)...")
                time.sleep(120)

            log_parts.append(f"\n--- Fluxo: {fluxo.nome} ({fluxo.tipo}) ---")

            # Pular fluxo cobrança_2_30 se não for dia de executar
            if fluxo.tipo == FLUXO_COBRANCA_2_30 and not dia_cobranca:
                log_parts.append(f"Pulando fluxo {fluxo.nome} — hoje não é dia de cobrança (ciclo a cada 3 dias)")
                # Criar execução com status "vazio"
                execucao = Execucao(
                    automacao_id=automacao.id,
                    fluxo_id=fluxo.id,
                    status="vazio",
                    registros_encontrados=total_registros,
                    registros_filtrados=0,
                    log=f"Fluxo {fluxo.nome} pulado — não é dia de cobrança (ciclo a cada 3 dias)",
                )
                db.add(execucao)
                db.commit()
                continue

            # Filtrar
            try:
                df_filtrado = _filtrar_por_fluxo(df, fluxo, col_venc, automacao)
            except ValueError as e:
                log_parts.append(f"Erro ao filtrar fluxo {fluxo.nome}: {e}")
                overall_status = "parcial"
                continue

            registros_filtrados = len(df_filtrado)
            total_filtrados += registros_filtrados
            log_parts.append(f"Registros filtrados: {registros_filtrados}")

            if registros_filtrados == 0:
                log_parts.append(f"Nenhum registro para o fluxo {fluxo.nome}. Planilha não atualizada.")
                # Criar execução com status "vazio"
                execucao = Execucao(
                    automacao_id=automacao.id,
                    fluxo_id=fluxo.id,
                    status="vazio",
                    registros_encontrados=total_registros,
                    registros_filtrados=0,
                    log=f"Fluxo {fluxo.nome}: 0 registros filtrados",
                )
                db.add(execucao)
                db.commit()
                continue

            # Converter para lista de dicts
            registros = df_filtrado.to_dict(orient="records")
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
                registros_encontrados=total_registros,
                registros_filtrados=registros_filtrados,
                log=f"Fluxo {fluxo.nome}: {resultado_sheets['log']}",
            )
            db.add(execucao)
            db.commit()

        return {
            "status": overall_status,
            "registros_encontrados": total_registros,
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