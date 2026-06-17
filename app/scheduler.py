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
from app.routers.executions import _running_automations, _mark_running, _clear_running, _purge_stale_running

_RELATORIO_ERP_SENHA = "Rcarol@2025"
_relatorio_lock = threading.Lock()

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

        # Limpar locks travados (>2h) antes de verificar
        _purge_stale_running()

        # Lock: verificar se já está em execução
        if automacao_id in _running_automations:
            msg = f"Automação {automacao.nome} (ID={automacao_id}) já está em execução. Pulando execução agendada."
            logger.warning(msg)
            notify_failure(automacao.nome, "Execução agendada pulada — lock ativo (automação ainda em execução ou travada). Acesse /saude para verificar ou use /api/clear-lock para liberar.")
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


def executar_relatorio_capitao_agendado():
    """Job diário: extrai snapshot do ERP e salva no banco. Pula se já existe dado de hoje."""
    if not _relatorio_lock.acquire(blocking=False):
        logger.info("Relatorio Capitão: extração já em andamento, pulando.")
        return

    from datetime import date as _date
    from app.services.relatorio_extractor import RelatorioExtractor
    from app.models import RelatorioCapitaoDiario
    from app.routers.relatorio import _salvar_snapshot

    db = SessionLocal()
    try:
        hoje = _date.today()
        existente = db.query(RelatorioCapitaoDiario).filter(RelatorioCapitaoDiario.data_ref == hoje).first()
        if existente and not existente.erros:
            logger.info(f"Relatorio Capitão: snapshot de {hoje} já existe e sem erros. Pulando.")
            return

        logger.info(f"Relatorio Capitão: iniciando extração para {hoje}...")
        extractor = RelatorioExtractor(_RELATORIO_ERP_SENHA)
        snap = extractor.extrair(hoje)
        _salvar_snapshot(db, snap)
        logger.info(f"Relatorio Capitão: extração concluída. erros={snap.erros or 'nenhum'}")
    except Exception as e:
        logger.error(f"Relatorio Capitão: falha na extração agendada: {e}")
    finally:
        db.close()
        _relatorio_lock.release()


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
            trigger=CronTrigger(hour=hora, minute=minuto, day_of_week=dias_cron, timezone=BRASILIA_TZ),
            id=f"automacao_{automacao.id}",
            name=f"Automação: {automacao.nome}",
            args=[automacao.id],
            replace_existing=True,
            max_instances=1,
            misfire_grace_time=3600,  # aceita execuções atrasadas em até 1h (ex: restart do container)
            coalesce=True,  # se perdeu N execuções, roda só 1 vez ao recuperar
        )
        logger.info(f"Agendado: {automacao.nome} às {hora:02d}:{minuto:02d} dias={dias_cron}")


def iniciar_scheduler(db: Session):
    """Inicializa o scheduler e carrega as automações."""
    atualizar_agendamentos(db)

    # Job fixo: relatório diário Regional Capitão — 08:00 e 20:00, seg a sex
    for job_id, hora in [("relatorio_capitao_manha", 8), ("relatorio_capitao_tarde", 20)]:
        scheduler.add_job(
            executar_relatorio_capitao_agendado,
            trigger=CronTrigger(hour=hora, minute=0, day_of_week="0-4", timezone=BRASILIA_TZ),
            id=job_id,
            name=f"Relatório Capitão {'08h' if hora == 8 else '20h'}",
            replace_existing=True,
            max_instances=1,
            misfire_grace_time=3600,
            coalesce=True,
        )
        logger.info(f"Agendado: Relatório Capitão às {hora:02d}:00 seg-sex")

    # Job WhatsApp relatório — carrega config do banco
    try:
        from app.routers.relatorio import _wz_config_get
        cfg = _wz_config_get(db)
        _reagendar_whatsapp(cfg)
    except Exception as e:
        logger.warning(f"WhatsApp job não agendado na inicialização: {e}")

    if not scheduler.running:
        scheduler.start()
        logger.info("Scheduler iniciado")


def executar_whatsapp_relatorio():
    """Envia relatório do dia via WhatsApp para os números configurados."""
    from datetime import date as _date
    from app.models import RelatorioWhatsappConfig
    from app.routers.relatorio import _buscar_snapshot, _snap_para_dict, _formatar_relatorio_wz, _enviar_wz, _wz_config_get

    db = SessionLocal()
    try:
        cfg = _wz_config_get(db)
        if not cfg.ativo or not cfg.server_url or not cfg.instance_token:
            return
        hoje = _date.today()
        snap = _buscar_snapshot(db, hoje)
        if not snap:
            logger.warning("WhatsApp: sem snapshot para hoje, pulando envio.")
            return
        msg = _formatar_relatorio_wz(_snap_para_dict(snap), hoje.strftime("%d/%m/%Y"))
        results = _enviar_wz(cfg, msg)
        logger.info(f"WhatsApp relatorio enviado: {results}")
    except Exception as e:
        logger.error(f"WhatsApp relatorio: erro no envio: {e}")
    finally:
        db.close()


def _reagendar_whatsapp(cfg):
    """Recria o job de WhatsApp no scheduler com a config atualizada."""
    try:
        scheduler.remove_job("whatsapp_relatorio")
    except Exception:
        pass
    if not cfg.ativo or not cfg.horario_envio:
        return
    try:
        hora, minuto = cfg.horario_envio.split(":")
        scheduler.add_job(
            executar_whatsapp_relatorio,
            trigger=CronTrigger(hour=int(hora), minute=int(minuto), day_of_week=cfg.dias_envio, timezone=BRASILIA_TZ),
            id="whatsapp_relatorio",
            name="WhatsApp Relatório Capitão",
            replace_existing=True,
            max_instances=1,
            misfire_grace_time=3600,
            coalesce=True,
        )
        logger.info(f"WhatsApp relatorio agendado: {cfg.horario_envio} dias={cfg.dias_envio}")
    except Exception as e:
        logger.error(f"Erro ao agendar WhatsApp: {e}")


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