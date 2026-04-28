import logging
import threading
from datetime import timedelta

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import SessionLocal, get_db
from app.models import Automacao, Execucao, Fluxo, ERPConfig
from app.services.processor import processar_automacao
from app.services.notifier import notify_failure
from app.tz import agora

logger = logging.getLogger(__name__)

router = APIRouter()

# Lock por automação + estado do que está rodando
_running_automations: set[int] = set()
# Mapeia automacao_id → info do fluxo que está rodando agora
_running_fluxo_info: dict[int, dict] = {}


def _run_automation_bg(automacao_id: int):
    """Roda a automação em background, atualizando o estado em tempo real."""
    db = SessionLocal()
    try:
        # Eager loading para evitar detached instances
        from sqlalchemy.orm import joinedload
        automacao = db.query(Automacao).options(
            joinedload(Automacao.erp_configs).joinedload(ERPConfig.fluxos)
        ).filter(Automacao.id == automacao_id).first()
        if not automacao or not automacao.ativo:
            return

        # Forçar carregamento de todos os relacionamentos antes de começar
        for erp in automacao.erp_configs:
            _ = erp.fluxos
            _ = erp.erp_tipo
            _ = erp.erp_url

        _running_fluxo_info[automacao_id] = {"fluxo": "Iniciando...", "erp": ""}

        resultado = processar_automacao(automacao, db, on_fluxo_start=_on_fluxo_start)
        logger.info(f"Automação {automacao.nome}: status={resultado['status']}")

    except Exception as e:
        logger.exception(f"Erro ao processar automação: {e}")
        notify_failure(f"Automação ID={automacao_id}", str(e))
    finally:
        _running_automations.discard(automacao_id)
        _running_fluxo_info.pop(automacao_id, None)
        db.close()


def _on_fluxo_start(automacao_id: int, erp_tipo: str, fluxo_nome: str):
    """Callback do processor quando um fluxo começa a rodar."""
    _running_fluxo_info[automacao_id] = {
        "fluxo": fluxo_nome,
        "erp": erp_tipo,
    }


@router.post("/executar/{automacao_id}")
def executar_automacao(automacao_id: int, db: Session = Depends(get_db)):
    automacao = db.query(Automacao).filter(Automacao.id == automacao_id).first()
    if not automacao:
        raise HTTPException(status_code=404)

    if automacao_id in _running_automations:
        return {
            "status": "ja_em_execucao",
            "message": f"Automação '{automacao.nome}' já está em execução.",
        }

    # Reservar o lock ANTES de iniciar a thread para evitar race condition
    _running_automations.add(automacao_id)
    thread = threading.Thread(
        target=_run_automation_bg,
        args=(automacao_id,),
        daemon=True,
    )
    thread.start()

    return {"status": "executando", "message": f"Automação '{automacao.nome}' iniciada."}


def _run_single_fluxo_bg(automacao_id: int, fluxo_id: int):
    """Roda apenas um fluxo específico em background."""
    db = SessionLocal()
    try:
        from sqlalchemy.orm import joinedload
        automacao = db.query(Automacao).options(
            joinedload(Automacao.erp_configs).joinedload(ERPConfig.fluxos)
        ).filter(Automacao.id == automacao_id).first()
        if not automacao or not automacao.ativo:
            return

        _running_fluxo_info[automacao_id] = {"fluxo": "Iniciando...", "erp": ""}

        from app.services.processor import processar_automacao_fluxo_unico
        resultado = processar_automacao_fluxo_unico(automacao, fluxo_id, db, on_fluxo_start=_on_fluxo_start)
        logger.info(f"Fluxo {fluxo_id}: status={resultado['status']}")

    except Exception as e:
        logger.exception(f"Erro ao processar fluxo {fluxo_id}: {e}")
        notify_failure(f"Fluxo ID={fluxo_id}", str(e))
    finally:
        _running_automations.discard(automacao_id)
        _running_fluxo_info.pop(automacao_id, None)
        db.close()


@router.post("/executar/{automacao_id}/fluxo/{fluxo_id}")
def executar_fluxo(automacao_id: int, fluxo_id: int, db: Session = Depends(get_db)):
    """Executar apenas um fluxo específico (para debug)."""
    automacao = db.query(Automacao).filter(Automacao.id == automacao_id).first()
    if not automacao:
        raise HTTPException(status_code=404, detail="Automação não encontrada")

    fluxo = db.query(Fluxo).filter(Fluxo.id == fluxo_id).first()
    if not fluxo:
        raise HTTPException(status_code=404, detail="Fluxo não encontrado")

    if automacao_id in _running_automations:
        return {
            "status": "ja_em_execucao",
            "message": f"Automação já está em execução.",
        }

    _running_automations.add(automacao_id)
    thread = threading.Thread(
        target=_run_single_fluxo_bg,
        args=(automacao_id, fluxo_id),
        daemon=True,
    )
    thread.start()

    return {"status": "executando", "message": f"Fluxo '{fluxo.nome}' iniciado."}


@router.post("/executar-todos")
def executar_todos(db: Session = Depends(get_db)):
    automacoes = db.query(Automacao).filter(Automacao.ativo == True).all()

    for automacao in automacoes:
        if automacao.id not in _running_automations:
            thread = threading.Thread(
                target=_run_automation_bg,
                args=(automacao.id,),
                daemon=True,
            )
            thread.start()

    return {"status": "executando", "message": f"{len(automacoes)} automações iniciadas."}