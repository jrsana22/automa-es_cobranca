"""
Scheduler — APScheduler integrado ao FastAPI para executar automações em horários definidos.
Cada automação roda todos os seus fluxos ativos de uma vez (exporta 1x, filtra Nx).
Inclui lock por automação (impede execução simultânea do mesmo automation_id).
"""

import logging
import os
import shutil
import threading
import time

from apscheduler.schedulers.background import BackgroundScheduler
from zoneinfo import ZoneInfo

BRASILIA_TZ = ZoneInfo("America/Sao_Paulo")
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Automacao, Execucao
from app.services.processor import processar_automacao
from app.services.notifier import notify_failure
from app.routers.executions import _running_automations, _mark_running, _clear_running

logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler(timezone=BRASILIA_TZ)


DB_PATH = "/app/data/automacao.db"
DB_BACKUP_PATH = "/app/data/automacao.db.bak"


def _backup_db():
    try:
        if os.path.exists(DB_PATH):
            shutil.copy2(DB_PATH, DB_BACKUP_PATH)
            logger.info("Backup do banco realizado.")
    except Exception as e:
        logger.warning(f"Falha no backup do banco: {e}")


def executar_automacao_agendada(automacao_id: int):
    """Callback executado pelo scheduler para uma automação específica."""
    db = SessionLocal()
    try:
        automacao = db.query(Automacao).filter(Automacao.id == automacao_id).first()
        if not automacao or not automacao.ativo:
            logger.info(f"Automação {automacao_id} não encontrada ou inativa. Pulando.")
            return

        # Lock: verificar se já está em execução
        if automacao_id in _running_automations:
            msg = f"Automação {automacao.nome} (ID={automacao_id}) já está em execução. Pulando."
            logger.warning(msg)
            return

        _mark_running(automacao_id)
        _backup_db()
        logger.info(f"Executando automação agendada: {automacao.nome} (ID={automacao_id})")

        try:
            from app.routers.executions import _on_fluxo_start
            resultado = processar_automacao(automacao, db, agendado=True, on_fluxo_start=_on_fluxo_start)
            logger.info(
                f"Automação {automacao.nome}: status={resultado['status']}, "
                f"encontrados={resultado.get('registros_encontrados', 0)}, "
                f"filtrados={resultado.get('registros_filtrados', 0)}"
            )
        except Exception as e:
            logger.exception(f"Erro ao processar automação {automacao.nome}: {e}")
            notify_failure(automacao.nome, str(e))

    except Exception as e:
        logger.exception(f"Erro ao executar automação agendada {automacao_id}: {e}")
        notify_failure(f"Automação ID={automacao_id}", str(e))
    finally:
        _clear_running(automacao_id)
        db.close()


def atualizar_agendamentos(db: Session):
    """
    Lê todas as automações ativas do banco e recria os jobs no scheduler.
    Chamada na inicialização e quando uma automação é criada/editada.
    """
    # Remover todos os jobs existentes
    scheduler.remove_all_jobs()

    # Criar jobs para cada automação ativa
    automacoes = db.query(Automacao).filter(Automacao.ativo == True).all()

    for automacao in automacoes:
        # Parse horário (formato HH:MM)
        try:
            hora, minuto = automacao.horario_execucao.split(":")
            hora = int(hora)
            minuto = int(minuto)
        except (ValueError, AttributeError):
            hora, minuto = 6, 0  # Default: 06:00

        # Parse dias da semana (formato APScheduler: 0=Seg, 6=Dom)
        dias_cron = automacao.dias_semana if automacao.dias_semana else "0,1,2,3,4"

        scheduler.add_job(
            executar_automacao_agendada,
            trigger=CronTrigger(hour=hora, minute=minuto, day_of_week=dias_cron),
            id=f"automacao_{automacao.id}",
            name=f"Automação: {automacao.nome}",
            args=[automacao.id],
            replace_existing=True,
        )
        logger.info(f"Agendado: {automacao.nome} às {hora:02d}:{minuto:02d} dias={dias_cron}")


def iniciar_scheduler(db: Session):
    """Inicializa o scheduler e carrega as automações."""
    atualizar_agendamentos(db)
    if not scheduler.running:
        scheduler.start()
        logger.info("Scheduler iniciado")


def iniciar_watchdog():
    """Inicia thread que monitora o scheduler e reinicia se morrer."""
    def _watchdog():
        while True:
            time.sleep(60)
            if not scheduler.running:
                logger.warning("Scheduler parou! Reiniciando...")
                try:
                    db = SessionLocal()
                    iniciar_scheduler(db)
                    db.close()
                    logger.info("Scheduler reiniciado pelo watchdog.")
                except Exception as e:
                    logger.error(f"Watchdog falhou ao reiniciar scheduler: {e}")

    t = threading.Thread(target=_watchdog, daemon=True, name="scheduler-watchdog")
    t.start()
    logger.info("Watchdog do scheduler iniciado.")