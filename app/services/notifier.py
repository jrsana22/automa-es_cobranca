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


def notify_failure(automation_name: str, error_message: str) -> bool:
    """
    Envia alerta de falha via WhatsApp usando a uazapi.

    Args:
        automation_name: Nome da automação que falhou.
        error_message: Mensagem de erro.

    Returns:
        True se o alerta foi enviado com sucesso, False caso contrário.
    """
    text = (
        f"⚠️ FALHA NA AUTOMAÇÃO\n\n"
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