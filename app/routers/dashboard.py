from fastapi import APIRouter, Request, Depends
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.database import get_db
from app.models import Automacao, Execucao, Fluxo, FLUXOS_PADRAO

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/", response_class=HTMLResponse)
def dashboard(request: Request, db: Session = Depends(get_db)):
    automacoes = db.query(Automacao).order_by(Automacao.nome).all()

    # Últimas execuções com fluxo
    ultimas_execucoes = (
        db.query(Execucao)
        .order_by(Execucao.data.desc())
        .limit(50)
        .all()
    )

    # Stats: registros filtrados hoje por tipo de fluxo
    from datetime import datetime, timedelta
    hoje = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    semana = hoje - timedelta(days=7)
    mes = hoje - timedelta(days=30)

    stats_hoje = db.query(Execucao).filter(
        Execucao.data >= hoje,
        Execucao.fluxo_id != None,
        Execucao.status.in_(["sucesso", "parcial", "vazio"]),
    ).all()

    stats_tipo = {}
    for exec in stats_hoje:
        tipo = exec.fluxo.tipo if exec.fluxo else "geral"
        if tipo not in stats_tipo:
            stats_tipo[tipo] = {"registros": 0, "execucoes": 0}
        stats_tipo[tipo]["registros"] += exec.registros_filtrados or 0
        stats_tipo[tipo]["execucoes"] += 1

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "automacoes": automacoes,
        "execucoes": ultimas_execucoes,
        "stats_tipo": stats_tipo,
    })


@router.get("/automacoes/novo", response_class=HTMLResponse)
def nova_automacao(request: Request):
    return templates.TemplateResponse("automacao_form.html", {
        "request": request,
        "automacao": None,
        "fluxos_padrao": FLUXOS_PADRAO,
    })


@router.get("/automacoes/{automacao_id}/editar", response_class=HTMLResponse)
def editar_automacao(automacao_id: int, request: Request, db: Session = Depends(get_db)):
    automacao = db.query(Automacao).filter(Automacao.id == automacao_id).first()
    if not automacao:
        return RedirectResponse("/", status_code=302)
    return templates.TemplateResponse("automacao_form.html", {
        "request": request,
        "automacao": automacao,
        "fluxos_padrao": FLUXOS_PADRAO,
    })


@router.get("/logs", response_class=HTMLResponse)
def logs(request: Request, db: Session = Depends(get_db)):
    execucoes = (
        db.query(Execucao)
        .outerjoin(Fluxo)
        .order_by(Execucao.data.desc())
        .limit(100)
        .all()
    )
    return templates.TemplateResponse("logs.html", {
        "request": request,
        "execucoes": execucoes,
    })