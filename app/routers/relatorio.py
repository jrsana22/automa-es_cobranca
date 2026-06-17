import logging
from datetime import date, datetime
from zoneinfo import ZoneInfo

from fastapi import APIRouter, BackgroundTasks, Depends, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.auth import require_auth
from app.database import SessionLocal, get_db
from app.models import RelatorioCapitaoDiario
from app.services.relatorio_extractor import RelatorioExtractor

logger = logging.getLogger(__name__)
router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

_BRASILIA = ZoneInfo("America/Sao_Paulo")
ERP_SENHA = "Rcarol@2025"

_extracao_em_andamento = False


def _mes_anterior(d: date) -> tuple[int, int]:
    if d.month == 1:
        return d.year - 1, 12
    return d.year, d.month - 1


def _snap_para_dict(r: RelatorioCapitaoDiario) -> dict:
    if r is None:
        return {}
    return {f: getattr(r, f) for f in [
        "data_ref", "extraido_em",
        "vendas_total", "vendas_capitao", "vendas_capitao2", "vendas_jardim_europa",
        "cotacoes_mes", "cotacoes_dia_anterior",
        "reativacao_qtd", "reativacao_valor",
        "pb_total", "pb_pagos", "pb_valor",
        "inadi_total_qtd", "inadi_total_valor",
        "inadi_mes_ant_qtd", "inadi_mes_ant_valor",
        "inadi_mes_atual_qtd", "inadi_mes_atual_valor",
        "cancelamento_qtd",
        "receb_total_qtd", "receb_total_valor",
        "receb_capitao_qtd", "receb_capitao_valor",
        "receb_capitao2_qtd", "receb_capitao2_valor",
        "receb_jardim_europa_qtd", "receb_jardim_europa_valor",
        "log", "erros", "manual",
    ]}


def _buscar_snapshot(db: Session, ref: date) -> RelatorioCapitaoDiario | None:
    return db.query(RelatorioCapitaoDiario).filter(RelatorioCapitaoDiario.data_ref == ref).first()


def _salvar_snapshot(db: Session, snap, manual: bool = False):
    existing = _buscar_snapshot(db, snap.data_ref)
    if existing:
        row = existing
    else:
        row = RelatorioCapitaoDiario(data_ref=snap.data_ref)
        db.add(row)

    for attr in [
        "vendas_total", "vendas_capitao", "vendas_capitao2", "vendas_jardim_europa",
        "cotacoes_mes", "cotacoes_dia_anterior",
        "reativacao_qtd", "reativacao_valor",
        "pb_total", "pb_pagos", "pb_valor",
        "inadi_total_qtd", "inadi_total_valor",
        "inadi_mes_ant_qtd", "inadi_mes_ant_valor",
        "inadi_mes_atual_qtd", "inadi_mes_atual_valor",
        "cancelamento_qtd",
        "receb_total_qtd", "receb_total_valor",
        "receb_capitao_qtd", "receb_capitao_valor",
        "receb_capitao2_qtd", "receb_capitao2_valor",
        "receb_jardim_europa_qtd", "receb_jardim_europa_valor",
        "log", "erros",
    ]:
        setattr(row, attr, getattr(snap, attr))

    row.extraido_em = datetime.now(_BRASILIA).replace(tzinfo=None)
    row.manual = manual
    db.commit()
    return row


def _run_extracao_background():
    global _extracao_em_andamento
    _extracao_em_andamento = True
    db = SessionLocal()
    try:
        extractor = RelatorioExtractor(ERP_SENHA)
        snap = extractor.extrair()
        _salvar_snapshot(db, snap)
        logger.info(f"Extração concluída: {snap.data_ref} erros={snap.erros}")
    except Exception as e:
        logger.error(f"Erro na extração background: {e}")
    finally:
        db.close()
        _extracao_em_andamento = False


@router.get("/relatorio", response_class=HTMLResponse)
def relatorio_dashboard(request: Request, db: Session = Depends(get_db), _: None = Depends(require_auth)):
    hoje = date.today()
    ano_ant, mes_ant_n = _mes_anterior(hoje)
    # Mesmo dia do mês passado
    try:
        mesmo_dia_mes_passado = date(ano_ant, mes_ant_n, hoje.day)
    except ValueError:
        import calendar
        last_day = calendar.monthrange(ano_ant, mes_ant_n)[1]
        mesmo_dia_mes_passado = date(ano_ant, mes_ant_n, last_day)

    snap_hoje = _buscar_snapshot(db, hoje)
    snap_mes_passado = _buscar_snapshot(db, mesmo_dia_mes_passado)

    historico = (
        db.query(RelatorioCapitaoDiario)
        .order_by(RelatorioCapitaoDiario.data_ref.desc())
        .limit(30)
        .all()
    )

    return templates.TemplateResponse("relatorio_capitao.html", {
        "request": request,
        "hoje": hoje.strftime("%d/%m/%Y"),
        "mesmo_dia_mes_passado": mesmo_dia_mes_passado.strftime("%d/%m/%Y"),
        "snap_hoje": _snap_para_dict(snap_hoje),
        "snap_mes_passado": _snap_para_dict(snap_mes_passado),
        "historico": [_snap_para_dict(r) for r in historico],
        "extracao_em_andamento": _extracao_em_andamento,
    })


@router.post("/relatorio/extrair")
def relatorio_extrair(background_tasks: BackgroundTasks, _: None = Depends(require_auth)):
    global _extracao_em_andamento
    if _extracao_em_andamento:
        return JSONResponse({"ok": False, "msg": "Extração já em andamento. Aguarde."}, status_code=409)
    background_tasks.add_task(_run_extracao_background)
    return JSONResponse({"ok": True, "msg": "Extração iniciada. Recarregue a página em ~60s."})


@router.post("/relatorio/manual")
def relatorio_manual(
    db: Session = Depends(get_db),
    _: None = Depends(require_auth),
    data_ref: str = Form(...),
    vendas_capitao: int = Form(0),
    vendas_capitao2: int = Form(0),
    vendas_jardim_europa: int = Form(0),
    reativacao_qtd: int = Form(0),
    reativacao_valor: float = Form(0.0),
    pb_total: int = Form(0),
    pb_pagos: int = Form(0),
    inadi_total_qtd: int = Form(0),
    inadi_total_valor: float = Form(0.0),
    receb_total_qtd: int = Form(0),
    receb_total_valor: float = Form(0.0),
    receb_capitao_qtd: int = Form(0),
    receb_capitao_valor: float = Form(0.0),
    receb_capitao2_qtd: int = Form(0),
    receb_capitao2_valor: float = Form(0.0),
    receb_jardim_europa_qtd: int = Form(0),
    receb_jardim_europa_valor: float = Form(0.0),
):
    from dataclasses import dataclass

    try:
        ref = date.fromisoformat(data_ref)
    except ValueError:
        return JSONResponse({"ok": False, "msg": "Data inválida"}, status_code=400)

    class FakeSnap:
        pass

    snap = FakeSnap()
    snap.data_ref = ref
    snap.vendas_total = vendas_capitao + vendas_capitao2 + vendas_jardim_europa
    snap.vendas_capitao = vendas_capitao
    snap.vendas_capitao2 = vendas_capitao2
    snap.vendas_jardim_europa = vendas_jardim_europa
    snap.cotacoes_mes = 0
    snap.cotacoes_dia_anterior = 0
    snap.reativacao_qtd = reativacao_qtd
    snap.reativacao_valor = reativacao_valor
    snap.pb_total = pb_total
    snap.pb_pagos = pb_pagos
    snap.pb_valor = 0.0
    snap.inadi_total_qtd = inadi_total_qtd
    snap.inadi_total_valor = inadi_total_valor
    snap.inadi_mes_ant_qtd = 0
    snap.inadi_mes_ant_valor = 0.0
    snap.inadi_mes_atual_qtd = 0
    snap.inadi_mes_atual_valor = 0.0
    snap.cancelamento_qtd = 0
    snap.receb_total_qtd = receb_total_qtd
    snap.receb_total_valor = receb_total_valor
    snap.receb_capitao_qtd = receb_capitao_qtd
    snap.receb_capitao_valor = receb_capitao_valor
    snap.receb_capitao2_qtd = receb_capitao2_qtd
    snap.receb_capitao2_valor = receb_capitao2_valor
    snap.receb_jardim_europa_qtd = receb_jardim_europa_qtd
    snap.receb_jardim_europa_valor = receb_jardim_europa_valor
    snap.log = "Entrada manual"
    snap.erros = ""

    _salvar_snapshot(db, snap, manual=True)
    return JSONResponse({"ok": True, "msg": f"Dados de {data_ref} salvos."})
