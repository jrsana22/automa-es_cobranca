"""
Notificador de falhas via WhatsApp (uazapi).
Envia alerta quando uma automação falha.
"""

import logging

from app.tz import agora

import requests

logger = logging.getLogger(__name__)

UAZAPI_URL = "https://wwwsolucoesdeia.uazapi.com/send/text"
UAZAPI_TOKEN = "9bd6907a-28b3-4c06-be3f-659b1f34c549"
NOTIFY_NUMBER = "553186058233"
NOTIFY_SUCCESS_NUMBER = "5531986058233"


def notify_result(automation_name: str, status: str) -> bool:
    """Envia notificação ao final de cada execução, independente do status."""
    if status == "sucesso":
        header = "✅ PLANILHA ATUALIZADA"
        body = "O preenchimento da planilha de cobrança foi concluído."
    elif status == "parcial":
        header = "⚠️ PLANILHA ATUALIZADA COM ALERTAS"
        body = "Alguns fluxos concluíram com erro. Verifique o painel."
    else:
        header = "❌ FALHA NA ATUALIZAÇÃO"
        body = "A automação encontrou erros. Verifique o painel."

    text = (
        f"{header}\n\n"
        f"Cliente: {automation_name}\n"
        f"{body}\n"
        f"Horário: {agora().strftime('%d/%m/%Y - %H:%M')}"
    )
    payload = {"number": NOTIFY_SUCCESS_NUMBER, "text": text}
    headers = {"token": UAZAPI_TOKEN, "Content-Type": "application/json"}
    try:
        resp = requests.post(UAZAPI_URL, json=payload, headers=headers, timeout=10)
        if resp.status_code == 200:
            logger.info(f"Notificação WhatsApp enviada: {automation_name} [{status}]")
            return True
        else:
            logger.error(f"Falha ao enviar notificação WhatsApp: {resp.status_code}")
            return False
    except Exception as e:
        logger.error(f"Erro ao enviar notificação WhatsApp: {e}")
        return False


def notify_success(automation_name: str) -> bool:
    return notify_result(automation_name, "sucesso")


def _classify_error(error_message: str) -> str:
    msg = error_message.lower()
    if any(x in msg for x in ["credencial", "login", "401", "unauthorized", "senha", "invalid"]):
        return "🔐 ERRO DE CREDENCIAL"
    elif any(x in msg for x in ["timeout", "connection", "network", "refused", "unreachable"]):
        return "🌐 ERRO DE REDE"
    elif any(x in msg for x in ["sheets", "google", "spreadsheet", "aba", "planilha"]):
        return "📊 ERRO NA PLANILHA"
    else:
        return "⚠️ FALHA NA AUTOMAÇÃO"


def notify_failure(automation_name: str, error_message: str) -> bool:
    """
    Envia alerta de falha via WhatsApp usando a uazapi.

    Args:
        automation_name: Nome da automação que falhou.
        error_message: Mensagem de erro.

    Returns:
        True se o alerta foi enviado com sucesso, False caso contrário.
    """
    tipo = _classify_error(error_message)
    text = (
        f"{tipo}\n\n"
        f"Cliente: {automation_name}\n"
        f"Erro: {error_message}\n"
        f"Horário: {agora().strftime('%d/%m/%Y %H:%M')}"
    )

    payload = {
        "number": NOTIFY_NUMBER,
        "text": text,
    }
    headers = {
        "token": UAZAPI_TOKEN,
        "Content-Type": "application/json",
    }

    try:
        resp = requests.post(UAZAPI_URL, json=payload, headers=headers, timeout=10)
        if resp.status_code == 200:
            logger.info(f"Alerta WhatsApp enviado para {automation_name}")
            return True
        else:
            logger.error(f"Falha ao enviar alerta WhatsApp: {resp.status_code} {resp.text}")
            return False
    except Exception as e:
        logger.error(f"Erro ao enviar alerta WhatsApp: {e}")
        return False