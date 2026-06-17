import calendar as _cal
import hashlib
import hmac
import logging
from datetime import date, datetime, timedelta
from math import ceil
from typing import Optional
from zoneinfo import ZoneInfo

from fastapi import APIRouter, BackgroundTasks, Depends, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

import json
import urllib.error
import urllib.request

from app.auth import require_auth
from app.config import settings
from app.database import SessionLocal, get_db
from app.models import RelatorioCapitaoDiario, RelatorioWhatsappConfig
from app.services.relatorio_extractor import RelatorioExtractor

logger = logging.getLogger(__name__)
router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

_BRASILIA = ZoneInfo("America/Sao_Paulo")
ERP_SENHA = "Rcarol@2025"


def _parse_data(data: str) -> Optional[date]:
    if not data:
        return None
    try:
        return date.fromisoformat(data)
    except ValueError:
        return None


def _client_key() -> str:
    """Chave derivada do SECRET_KEY para acesso público do cliente. Estável, não adivinhável."""
    return hmac.new(
        settings.SECRET_KEY.encode(),
        b"relatorio-capitao-cliente",
        hashlib.sha256,
    ).hexdigest()[:24]

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


def _fmt_data_label(iso_str: str, hoje: date) -> str:
    try:
        d = date.fromisoformat(iso_str)
        if d == hoje:
            return f"Hoje — {d.strftime('%d/%m')}"
        if d == hoje - timedelta(days=1):
            return f"Ontem — {d.strftime('%d/%m')}"
        dias = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
        return f"{dias[d.weekday()]} {d.strftime('%d/%m')}"
    except Exception:
        return iso_str


def _gerar_insights(snap: dict, data_ref: date) -> list[dict]:
    if not snap:
        return []
    insights = []
    day_n = data_ref.day
    days_in_month = _cal.monthrange(data_ref.year, data_ref.month)[1]

    vendas_total = snap.get("vendas_total") or 0
    vendas_cap  = snap.get("vendas_capitao") or 0
    vendas_cap2 = snap.get("vendas_capitao2") or 0
    vendas_je   = snap.get("vendas_jardim_europa") or 0

    pb_total = snap.get("pb_total") or 0
    pb_pagos = snap.get("pb_pagos") or 0

    reativ    = snap.get("reativacao_qtd") or 0
    inadi_tot = snap.get("inadi_total_qtd") or 0
    inadi_ma  = snap.get("inadi_mes_atual_qtd") or 0
    inadi_mp  = snap.get("inadi_mes_ant_qtd") or 0

    rc  = snap.get("receb_capitao_valor") or 0.0
    rc2 = snap.get("receb_capitao2_valor") or 0.0
    rje = snap.get("receb_jardim_europa_valor") or 0.0
    receb_tot = snap.get("receb_total_valor") or 0.0

    # 1. Projeção de vendas
    if day_n > 0 and vendas_total > 0:
        proj = round(vendas_total / day_n * days_in_month)
        insights.append({
            "icon": "📈",
            "text": f"Ritmo atual projeta {proj} vendas até o fim do mês ({vendas_total} em {day_n} dias)",
            "level": "info",
        })

    # 2. 1º Boleto vs meta 70%
    META_PB = 70
    if pb_total > 0:
        pct = pb_pagos / pb_total * 100
        if pct >= META_PB:
            insights.append({"icon": "✅", "text": f"1º Boleto acima da meta: {pct:.0f}% de conversão ({pb_pagos}/{pb_total} pagos)", "level": "good"})
        else:
            faltam = ceil(pb_total * META_PB / 100) - pb_pagos
            insights.append({"icon": "⚠️", "text": f"1º Boleto em {pct:.0f}% — faltam {faltam} pagamentos para atingir {META_PB}%", "level": "warn"})

    # 3. Taxa de recuperação
    pool = reativ + inadi_tot
    if pool > 0:
        taxa = reativ / pool * 100
        if taxa >= 35:
            insights.append({"icon": "✅", "text": f"Recuperação saudável: {taxa:.0f}% dos inadimplentes foram reativados ({reativ} de {pool})", "level": "good"})
        else:
            insights.append({"icon": "🔴", "text": f"Recuperação baixa: apenas {taxa:.0f}% dos inadimplentes reativados ({reativ} de {pool})", "level": "alert"})

    # 4. Concentração de inadimplência
    if inadi_ma > 0 and inadi_mp > 0 and inadi_ma > inadi_mp * 2:
        insights.append({"icon": "🔴", "text": f"Inadimplência concentrada no mês atual: {inadi_ma} contratos vs {inadi_mp} do mês anterior", "level": "alert"})
    elif inadi_mp > 30:
        insights.append({"icon": "⚠️", "text": f"{inadi_mp} contratos do mês passado ainda em aberto — priorizar cobrança de vencidos", "level": "warn"})

    # 5. Líder de vendas
    if vendas_total > 0:
        ranking = sorted([("Capitão", vendas_cap), ("Capitão 2", vendas_cap2), ("Jardim Europa", vendas_je)], key=lambda x: x[1], reverse=True)
        lider, lider_qtd = ranking[0]
        ultimo, ultimo_qtd = ranking[-1]
        insights.append({"icon": "🏆", "text": f"{lider} lidera vendas com {lider_qtd} contratos no mês", "level": "info"})
        if lider_qtd >= 5 and ultimo_qtd < lider_qtd * 0.35:
            insights.append({"icon": "⚠️", "text": f"{ultimo} abaixo do esperado: {ultimo_qtd} vendas vs {lider_qtd} do líder", "level": "warn"})

    # 6. Recebimento: regional com maior arrecadação
    if receb_tot > 0 and rc + rc2 + rje > 0:
        lider_r = max([("Capitão", rc), ("Capitão 2", rc2), ("Jardim Europa", rje)], key=lambda x: x[1])
        pct_r = lider_r[1] / receb_tot * 100
        insights.append({"icon": "💰", "text": f"{lider_r[0]} concentra {pct_r:.0f}% do recebimento (R$ {lider_r[1]:,.0f})", "level": "info"})

    return insights


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
def relatorio_dashboard(request: Request, data: str = "", db: Session = Depends(get_db), _: None = Depends(require_auth)):
    data_ref = _parse_data(data)
    ctx = _contexto_relatorio(db, data_ref)
    ctx["request"] = request
    ctx["client_key"] = _client_key()
    return templates.TemplateResponse("relatorio_capitao.html", ctx)


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


def _contexto_relatorio(db: Session, data_ref: Optional[date] = None) -> dict:
    """Monta o contexto compartilhado entre a view admin e a view cliente."""
    import calendar as cal_mod
    data_ref = data_ref or date.today()
    hoje_real = date.today()
    ano_ant, mes_ant_n = _mes_anterior(data_ref)
    try:
        mesmo_dia_mes_passado = date(ano_ant, mes_ant_n, data_ref.day)
    except ValueError:
        last_day = cal_mod.monthrange(ano_ant, mes_ant_n)[1]
        mesmo_dia_mes_passado = date(ano_ant, mes_ant_n, last_day)

    snap_hoje = _buscar_snapshot(db, data_ref)
    snap_mes_passado = _buscar_snapshot(db, mesmo_dia_mes_passado)
    historico = (
        db.query(RelatorioCapitaoDiario)
        .order_by(RelatorioCapitaoDiario.data_ref.desc())
        .limit(30)
        .all()
    )
    datas_disponiveis = [str(r.data_ref) for r in historico]
    snap_dict = _snap_para_dict(snap_hoje)
    insights = _gerar_insights(snap_dict, data_ref)
    datas_fmt = [(d, _fmt_data_label(d, hoje_real)) for d in datas_disponiveis]
    return {
        "data_ref": data_ref.strftime("%Y-%m-%d"),
        "data_ref_fmt": data_ref.strftime("%d/%m/%Y"),
        "hoje": hoje_real.strftime("%d/%m/%Y"),
        "hoje_iso": str(hoje_real),
        "mesmo_dia_mes_passado": mesmo_dia_mes_passado.strftime("%d/%m/%Y"),
        "mesmo_dia_mes_passado_iso": str(mesmo_dia_mes_passado),
        "snap_hoje": snap_dict,
        "snap_mes_passado": _snap_para_dict(snap_mes_passado),
        "historico": [_snap_para_dict(r) for r in historico],
        "extracao_em_andamento": _extracao_em_andamento,
        "datas_disponiveis": datas_disponiveis,
        "datas_fmt": datas_fmt,
        "eh_hoje": data_ref == hoje_real,
        "insights": insights,
    }


@router.get("/relatorio/cliente", response_class=HTMLResponse)
def relatorio_cliente(request: Request, key: str = "", data: str = "", regional: str = "", db: Session = Depends(get_db)):
    expected = _client_key()
    if not hmac.compare_digest(key, expected):
        return HTMLResponse("<h2>Acesso negado</h2>", status_code=403)
    data_ref = _parse_data(data)
    ctx = _contexto_relatorio(db, data_ref)
    ctx["request"] = request
    ctx["client_key"] = key
    ctx["regional"] = regional if regional in ("capitao", "capitao2", "jardim") else "all"
    return templates.TemplateResponse("relatorio_capitao_cliente.html", ctx)


@router.get("/relatorio/link-cliente", response_class=HTMLResponse)
def relatorio_link_cliente(_: None = Depends(require_auth)):
    key = _client_key()
    base = settings.APP_BASE_URL.rstrip("/")
    url = f"{base}/relatorio/cliente?key={key}"
    return HTMLResponse(
        f"<html><body style='font-family:monospace;padding:2rem;background:#111;color:#eee'>"
        f"<p style='margin-bottom:1rem;color:#9ca3af;font-size:.9rem'>URL pública para o cliente (bookmarkar):</p>"
        f"<p style='word-break:break-all;font-size:1.1rem;color:#60a5fa'>{url}</p>"
        f"<p style='margin-top:1.5rem'><a href='/relatorio' style='color:#9ca3af'>← Voltar</a></p>"
        f"</body></html>"
    )


# ── WhatsApp helpers ────────────────────────────────────────────────────────

def _wz_config_get(db: Session) -> RelatorioWhatsappConfig:
    cfg = db.query(RelatorioWhatsappConfig).first()
    if not cfg:
        cfg = RelatorioWhatsappConfig()
        db.add(cfg)
        db.commit()
        db.refresh(cfg)
    return cfg


def _formatar_relatorio_wz(snap: dict, data_ref_fmt: str) -> str:
    v  = snap.get("vendas_total", 0) or 0
    vc = snap.get("vendas_capitao", 0) or 0
    vc2= snap.get("vendas_capitao2", 0) or 0
    vj = snap.get("vendas_jardim_europa", 0) or 0
    r  = snap.get("reativacao_qtd", 0) or 0
    rv = snap.get("reativacao_valor", 0.0) or 0.0
    pb = snap.get("pb_pagos", 0) or 0
    pbt= snap.get("pb_total", 0) or 0
    pct= round(pb / pbt * 100) if pbt > 0 else 0
    rec_q = snap.get("receb_total_qtd", 0) or 0
    rec_v = snap.get("receb_total_valor", 0.0) or 0.0
    rec_c = snap.get("receb_capitao_qtd", 0) or 0
    rec_c2= snap.get("receb_capitao2_qtd", 0) or 0
    rec_j = snap.get("receb_jardim_europa_qtd", 0) or 0
    in_t  = snap.get("inadi_total_qtd", 0) or 0
    in_mp = snap.get("inadi_mes_ant_qtd", 0) or 0
    in_ma = snap.get("inadi_mes_atual_qtd", 0) or 0

    return (
        f"📊 *Relatório Regional Capitão — {data_ref_fmt}*\n\n"
        f"📈 *Vendas:* {v} contratos\n"
        f"  Cap: {vc} | Cap 2: {vc2} | JE: {vj}\n\n"
        f"🔄 *Reativação:* {r} (R$ {rv:,.2f})\n\n"
        f"📋 *1º Boleto:* {pb}/{pbt} pagos ({pct}%)\n\n"
        f"💰 *Recebimento:* {rec_q} placas (R$ {rec_v:,.2f})\n"
        f"  Cap: {rec_c} | Cap 2: {rec_c2} | JE: {rec_j}\n\n"
        f"⚠️ *Inadimplência:* {in_t} contratos\n"
        f"  Mês passado: {in_mp} | Mês atual: {in_ma}\n\n"
        f"_Gerado automaticamente — {data_ref_fmt}_"
    )


def _enviar_wz(cfg: RelatorioWhatsappConfig, msg: str) -> list[dict]:
    results = []
    for numero in [cfg.numero_1, cfg.numero_2, cfg.numero_3]:
        if not numero:
            continue
        payload = json.dumps({"number": numero, "text": msg}).encode()
        req = urllib.request.Request(
            f"{cfg.server_url.rstrip('/')}/send/text",
            data=payload,
            headers={"token": cfg.instance_token, "Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                results.append({"numero": numero, "ok": True, "status": r.status})
        except urllib.error.HTTPError as e:
            results.append({"numero": numero, "ok": False, "status": e.code})
        except Exception as ex:
            results.append({"numero": numero, "ok": False, "erro": str(ex)})
    return results


def _wz_status(cfg: RelatorioWhatsappConfig) -> dict:
    if not cfg.server_url or not cfg.instance_token:
        return {"conectado": False, "motivo": "Não configurado"}
    try:
        req = urllib.request.Request(
            f"{cfg.server_url.rstrip('/')}/status",
            headers={"token": cfg.instance_token},
        )
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read())
            status = data.get("status", {})
            inst = status.get("checked_instance", {})
            conectado = inst.get("connection_status") == "connected"
            return {
                "conectado": conectado,
                "nome_instancia": inst.get("name", ""),
                "servidor": status.get("server_status", ""),
            }
    except Exception as ex:
        return {"conectado": False, "motivo": str(ex)}


# ── Client-side routes (authed by key) ─────────────────────────────────────

def _validar_key(key: str) -> bool:
    return hmac.compare_digest(key, _client_key())


@router.post("/relatorio/cliente/entrada")
def cliente_entrada(
    db: Session = Depends(get_db),
    key: str = Form(...),
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
    inadi_mes_ant_qtd: int = Form(0),
    inadi_mes_ant_valor: float = Form(0.0),
    inadi_mes_atual_qtd: int = Form(0),
    inadi_mes_atual_valor: float = Form(0.0),
    receb_total_qtd: int = Form(0),
    receb_total_valor: float = Form(0.0),
    receb_capitao_qtd: int = Form(0),
    receb_capitao_valor: float = Form(0.0),
    receb_capitao2_qtd: int = Form(0),
    receb_capitao2_valor: float = Form(0.0),
    receb_jardim_europa_qtd: int = Form(0),
    receb_jardim_europa_valor: float = Form(0.0),
):
    if not _validar_key(key):
        return JSONResponse({"ok": False, "msg": "Acesso negado"}, status_code=403)

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
    snap.inadi_mes_ant_qtd = inadi_mes_ant_qtd
    snap.inadi_mes_ant_valor = inadi_mes_ant_valor
    snap.inadi_mes_atual_qtd = inadi_mes_atual_qtd
    snap.inadi_mes_atual_valor = inadi_mes_atual_valor
    snap.cancelamento_qtd = 0
    snap.receb_total_qtd = receb_total_qtd
    snap.receb_total_valor = receb_total_valor
    snap.receb_capitao_qtd = receb_capitao_qtd
    snap.receb_capitao_valor = receb_capitao_valor
    snap.receb_capitao2_qtd = receb_capitao2_qtd
    snap.receb_capitao2_valor = receb_capitao2_valor
    snap.receb_jardim_europa_qtd = receb_jardim_europa_qtd
    snap.receb_jardim_europa_valor = receb_jardim_europa_valor
    snap.log = "Entrada manual (cliente)"
    snap.erros = ""

    _salvar_snapshot(db, snap, manual=True)
    return JSONResponse({"ok": True, "msg": f"Dados de {ref.strftime('%d/%m/%Y')} salvos."})


@router.get("/relatorio/cliente/wz-status")
def cliente_wz_status(key: str = "", db: Session = Depends(get_db)):
    if not _validar_key(key):
        return JSONResponse({"ok": False}, status_code=403)
    cfg = _wz_config_get(db)
    return JSONResponse({"ok": True, **_wz_status(cfg)})


def _add55(n: str) -> str:
    n = "".join(c for c in n if c.isdigit())
    if not n:
        return ""
    return n if n.startswith("55") else "55" + n


def _strip55(n: str) -> str:
    return n[2:] if n and n.startswith("55") else n


@router.post("/relatorio/cliente/wz-config")
async def cliente_wz_config(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    key = form.get("key", "")
    if not _validar_key(key):
        return JSONResponse({"ok": False, "msg": "Acesso negado"}, status_code=403)

    dias_list = form.getlist("dias")
    dias_envio = ",".join(sorted(dias_list, key=lambda x: int(x))) if dias_list else "0"

    cfg = _wz_config_get(db)
    cfg.numero_1 = _add55(form.get("numero_1", ""))
    cfg.numero_2 = _add55(form.get("numero_2", ""))
    cfg.numero_3 = _add55(form.get("numero_3", ""))
    cfg.horario_envio = form.get("horario_envio", "07:00")
    cfg.dias_envio = dias_envio
    cfg.ativo = form.get("ativo") == "true"
    cfg.atualizado_em = datetime.now(_BRASILIA).replace(tzinfo=None)
    db.commit()

    try:
        from app.scheduler import _reagendar_whatsapp
        _reagendar_whatsapp(cfg)
    except Exception:
        pass

    return JSONResponse({"ok": True, "msg": "Configuração salva."})


@router.post("/relatorio/cliente/wz-enviar")
def cliente_wz_enviar(
    db: Session = Depends(get_db),
    key: str = Form(...),
    data_ref: str = Form(""),
):
    if not _validar_key(key):
        return JSONResponse({"ok": False, "msg": "Acesso negado"}, status_code=403)

    ref = _parse_data(data_ref) or date.today()
    snap = _buscar_snapshot(db, ref)
    if not snap:
        return JSONResponse({"ok": False, "msg": f"Sem dados para {ref.strftime('%d/%m/%Y')}"}, status_code=404)

    cfg = _wz_config_get(db)
    if not cfg.server_url or not cfg.instance_token:
        return JSONResponse({"ok": False, "msg": "WhatsApp não configurado"}, status_code=400)

    msg = _formatar_relatorio_wz(_snap_para_dict(snap), ref.strftime("%d/%m/%Y"))
    results = _enviar_wz(cfg, msg)
    enviados = sum(1 for r in results if r.get("ok"))
    return JSONResponse({"ok": True, "enviados": enviados, "detalhes": results})


@router.get("/relatorio/cliente/wz-cfg")
def cliente_wz_cfg_get(key: str = "", db: Session = Depends(get_db)):
    if not _validar_key(key):
        return JSONResponse({"ok": False}, status_code=403)
    cfg = _wz_config_get(db)
    return JSONResponse({
        "ok": True,
        "numero_1": _strip55(cfg.numero_1),
        "numero_2": _strip55(cfg.numero_2),
        "numero_3": _strip55(cfg.numero_3),
        "horario_envio": cfg.horario_envio,
        "dias_envio": cfg.dias_envio,
        "ativo": cfg.ativo,
    })


# ── Admin WhatsApp routes (requer autenticação admin) ───────────────────────

@router.get("/relatorio/admin/wz-cfg")
def admin_wz_cfg_get(db: Session = Depends(get_db), _: None = Depends(require_auth)):
    cfg = _wz_config_get(db)
    return JSONResponse({
        "ok": True,
        "server_url": cfg.server_url or "",
        "instance_token": cfg.instance_token or "",
    })


@router.get("/relatorio/admin/wz-status")
def admin_wz_status(db: Session = Depends(get_db), _: None = Depends(require_auth)):
    cfg = _wz_config_get(db)
    return JSONResponse({"ok": True, **_wz_status(cfg)})


@router.post("/relatorio/admin/wz-config")
def admin_wz_config_save(
    db: Session = Depends(get_db),
    _: None = Depends(require_auth),
    server_url: str = Form(""),
    instance_token: str = Form(""),
):
    cfg = _wz_config_get(db)
    cfg.server_url = server_url.strip()
    cfg.instance_token = instance_token.strip()
    cfg.atualizado_em = datetime.now(_BRASILIA).replace(tzinfo=None)
    db.commit()

    try:
        from app.scheduler import _reagendar_whatsapp
        _reagendar_whatsapp(cfg)
    except Exception:
        pass

    return JSONResponse({"ok": True, "msg": "Credenciais salvas com sucesso."})
