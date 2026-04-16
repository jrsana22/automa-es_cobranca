from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Automacao, Execucao, Fluxo
from app.services.processor import processar_automacao
from app.services.notifier import notify_failure
from app.scheduler import _running_automations

router = APIRouter()


def _executar_automacao(automacao_id: int, db: Session) -> dict:
    """Lógica compartilhada de execução com lock e registro."""
    automacao = db.query(Automacao).filter(Automacao.id == automacao_id).first()
    if not automacao:
        raise HTTPException(status_code=404)

    # Lock: verificar se já está em execução
    if automacao_id in _running_automations:
        return {
            "execucao_id": None,
            "status": "ja_em_execucao",
            "message": f"Automação '{automacao.nome}' já está em execução.",
        }

    _running_automations.add(automacao_id)

    try:
        resultado = processar_automacao(automacao, db)
        return resultado
    except Exception as e:
        notify_failure(automacao.nome, str(e))
        return {
            "status": "erro",
            "registros_encontrados": 0,
            "registros_filtrados": 0,
            "log": str(e),
        }
    finally:
        _running_automations.discard(automacao_id)


@router.post("/executar/{automacao_id}")
def executar_automacao(automacao_id: int, db: Session = Depends(get_db)):
    return _executar_automacao(automacao_id, db)


@router.post("/executar/{automacao_id}/fluxo/{fluxo_id}")
def executar_fluxo(automacao_id: int, fluxo_id: int, db: Session = Depends(get_db)):
    """Executar apenas um fluxo específico (para debug)."""
    automacao = db.query(Automacao).filter(Automacao.id == automacao_id).first()
    if not automacao:
        raise HTTPException(status_code=404, detail="Automação não encontrada")

    fluxo = db.query(Fluxo).filter(
        Fluxo.id == fluxo_id,
        Fluxo.automacao_id == automacao_id,
    ).first()
    if not fluxo:
        raise HTTPException(status_code=404, detail="Fluxo não encontrado")

    # Por enquanto, executa a automação completa (que já filtra por todos os fluxos ativos)
    # Para executar um fluxo isolado, ativamos só ele temporariamente
    resultado = _executar_automacao(automacao_id, db)
    return resultado


@router.post("/executar-todos")
def executar_todos(db: Session = Depends(get_db)):
    automacoes = db.query(Automacao).filter(Automacao.ativo == True).all()
    resultados = []

    for automacao in automacoes:
        resultado = _executar_automacao(automacao.id, db)
        resultados.append({
            "automacao_id": automacao.id,
            "automacao_nome": automacao.nome,
            **resultado,
        })

    return {"resultados": resultados}