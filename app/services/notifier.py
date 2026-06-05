"""
Notificador via webhook n8n.
O n8n recebe o payload e dispara o WhatsApp, garantindo visibilidade total no histórico de execuções.
"""

import logging
import os

from app.tz import agora

import requests

logger = logging.getLogger(__name__)

N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL", "")


def build_fluxo_resumo_text(fluxo_resumo: list) -> str:
    """Formata lista de (nome, count) para exibição no WhatsApp."""
    if not fluxo_resumo:
        return ""
    lines = []
    for nome, count in fluxo_resumo:
        lines.append(f"• {nome}: {count} registro{'s' if count != 1 else ''}")
    return " | ".join(lines)


def _post_n8n(payload: dict) -> bool:
    if not N8N_WEBHOOK_URL:
        logger.error("N8N_WEBHOOK_URL não configurado — notificação ignorada")
        return False
    try:
        resp = requests.post(N8N_WEBHOOK_URL, json=payload, timeout=15)
        if resp.status_code < 300:
            logger.info(f"Webhook n8n enviado: {payload.get('nome')} [{payload.get('status')}]")
            return True
        else:
            logger.error(f"Webhook n8n retornou {resp.status_code}: {resp.text[:200]}")
            return False
    except Exception as e:
        logger.error(f"Erro ao chamar webhook n8n: {e}")
        return False


def notify_result(
    automation_name: str,
    status: str,
    fluxo_resumo: list | None = None,
    whatsapp_destinatario: str | None = None,
) -> bool:
    resumo = build_fluxo_resumo_text(fluxo_resumo or [])
    payload = {
        "nome": automation_name,
        "status": status,
        "whatsapp": whatsapp_destinatario or "",
        "resumo": resumo,
        "horario": agora().strftime("%d/%m/%Y - %H:%M"),
    }
    return _post_n8n(payload)


def notify_success(automation_name: str, whatsapp_destinatario: str | None = None) -> bool:
    return notify_result(automation_name, "sucesso", whatsapp_destinatario=whatsapp_destinatario)


def _classify_error(error_message: str) -> str:
    msg = error_message.lower()
    if any(x in msg for x in ["credencial", "login", "401", "unauthorized", "senha", "invalid"]):
        return "credencial"
    elif any(x in msg for x in ["timeout", "connection", "network", "refused", "unreachable"]):
        return "rede"
    elif any(x in msg for x in ["sheets", "google", "spreadsheet", "aba", "planilha"]):
        return "planilha"
    else:
        return "generico"


def notify_failure(
    automation_name: str,
    error_message: str,
    whatsapp_destinatario: str | None = None,
) -> bool:
    payload = {
        "nome": automation_name,
        "status": "erro",
        "tipo_erro": _classify_error(error_message),
        "whatsapp": whatsapp_destinatario or "",
        "resumo": error_message[:300],
        "horario": agora().strftime("%d/%m/%Y - %H:%M"),
    }
    return _post_n8n(payload)
