"""Timezone centralizado — Horário de Brasília (America/Sao_Paulo)."""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

BRASILIA = ZoneInfo("America/Sao_Paulo")


def agora() -> datetime:
    """Retorna datetime atual no fuso de Brasília (aware)."""
    return datetime.now(BRASILIA)


def hoje() -> datetime:
    """Retorna meia-noite de hoje no fuso de Brasília."""
    return agora().replace(hour=0, minute=0, second=0, microsecond=0)