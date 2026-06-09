import secrets
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.config import settings
from app.crypto import encrypt_password
from app.database import get_db
from app.models import (
    Automacao,
    ERPConfig,
    Fluxo,
    FormToken,
    _agora_brasilia,
    get_fluxos_padrao,
)
from app.scheduler import atualizar_agendamentos

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

_BRASILIA = ZoneInfo("America/Sao_Paulo")

_DEFAULT_URLS = {
    "apvs_brasil": lambda: settings.APVS_BRASIL_URL,
    "apvs_truck": lambda: settings.APVS_TRUCK_URL,
}


@router.post("/api/automacoes/{automacao_id}/gerar-token")
def gerar_token(automacao_id: int, db: Session = Depends(get_db)):
    automacao = db.query(Automacao).filter(Automacao.id == automacao_id).first()
    if not automacao:
        raise HTTPException(status_code=404)

    token = secrets.token_urlsafe(32)
    expires_at = datetime.now(_BRASILIA).replace(tzinfo=None) + timedelta(hours=72)

    form_token = FormToken(
        token=token,
        automacao_id=automacao_id,
        expires_at=expires_at,
        created_at=_agora_brasilia(),
    )
    db.add(form_token)
    db.commit()

    base_url = settings.APP_BASE_URL.rstrip("/")
    return {"url": f"{base_url}/form/{token}", "expires_em": "72 horas"}


@router.get("/form/{token}", response_class=HTMLResponse)
def form_credenciais(token: str, request: Request, db: Session = Depends(get_db)):
    form_token = db.query(FormToken).filter(FormToken.token == token).first()
    if not form_token:
        return templates.TemplateResponse(
            "credenciais_form.html", {"request": request, "estado": "invalido", "automacao": None}
        )

    agora = _agora_brasilia()
    if form_token.used:
        return templates.TemplateResponse(
            "credenciais_form.html", {"request": request, "estado": "usado", "automacao": None}
        )
    if form_token.expires_at < agora:
        return templates.TemplateResponse(
            "credenciais_form.html", {"request": request, "estado": "expirado", "automacao": None}
        )

    automacao = db.query(Automacao).filter(Automacao.id == form_token.automacao_id).first()
    if not automacao:
        raise HTTPException(status_code=404)

    erp_brasil = next((e for e in automacao.erp_configs if e.erp_tipo == "apvs_brasil"), None)
    erp_truck = next((e for e in automacao.erp_configs if e.erp_tipo == "apvs_truck"), None)

    return templates.TemplateResponse(
        "credenciais_form.html",
        {
            "request": request,
            "estado": "ok",
            "automacao": automacao,
            "token": token,
            "erp_brasil": erp_brasil,
            "erp_truck": erp_truck,
        },
    )


@router.post("/form/{token}", response_class=HTMLResponse)
def salvar_credenciais(
    token: str,
    request: Request,
    brasil_login: str = Form(""),
    brasil_senha: str = Form(""),
    truck_login: str = Form(""),
    truck_senha: str = Form(""),
    db: Session = Depends(get_db),
):
    form_token = db.query(FormToken).filter(FormToken.token == token).first()
    if not form_token:
        return templates.TemplateResponse(
            "credenciais_form.html", {"request": request, "estado": "invalido", "automacao": None}
        )

    agora = _agora_brasilia()
    if form_token.used:
        return templates.TemplateResponse(
            "credenciais_form.html", {"request": request, "estado": "usado", "automacao": None}
        )
    if form_token.expires_at < agora:
        return templates.TemplateResponse(
            "credenciais_form.html", {"request": request, "estado": "expirado", "automacao": None}
        )

    automacao = db.query(Automacao).filter(Automacao.id == form_token.automacao_id).first()
    if not automacao:
        raise HTTPException(status_code=404)

    erp_brasil = next((e for e in automacao.erp_configs if e.erp_tipo == "apvs_brasil"), None)
    erp_truck = next((e for e in automacao.erp_configs if e.erp_tipo == "apvs_truck"), None)

    erps_a_salvar = []
    if brasil_login.strip() and brasil_senha.strip():
        erps_a_salvar.append(("apvs_brasil", brasil_login.strip(), brasil_senha.strip()))
    if truck_login.strip() and truck_senha.strip():
        erps_a_salvar.append(("apvs_truck", truck_login.strip(), truck_senha.strip()))

    if not erps_a_salvar:
        return templates.TemplateResponse(
            "credenciais_form.html",
            {
                "request": request,
                "estado": "ok",
                "automacao": automacao,
                "token": token,
                "erp_brasil": erp_brasil,
                "erp_truck": erp_truck,
                "erro": "Preencha login e senha de pelo menos um sistema.",
            },
        )

    for erp_tipo, login, senha in erps_a_salvar:
        erp_config = next((e for e in automacao.erp_configs if e.erp_tipo == erp_tipo), None)
        if erp_config:
            erp_config.erp_login = login
            erp_config.erp_senha = encrypt_password(senha)
        else:
            erp_config = ERPConfig(
                automacao_id=automacao.id,
                erp_tipo=erp_tipo,
                erp_url=_DEFAULT_URLS[erp_tipo](),
                erp_login=login,
                erp_senha=encrypt_password(senha),
                ativo=True,
            )
            db.add(erp_config)
            db.flush()

            for fluxo_padrao in get_fluxos_padrao(erp_tipo):
                db.add(Fluxo(
                    erp_config_id=erp_config.id,
                    tipo=fluxo_padrao["tipo"],
                    nome=fluxo_padrao["nome"],
                    sheets_aba=fluxo_padrao["sheets_aba"],
                    filtro_dias_min=fluxo_padrao["filtro_dias_min"],
                    filtro_dias_max=fluxo_padrao["filtro_dias_max"],
                    formulario_id=fluxo_padrao["formulario_id"],
                    situacao_id=fluxo_padrao["situacao_id"],
                    ativo=True,
                ))

    form_token.used = True
    db.commit()
    atualizar_agendamentos(db)

    return templates.TemplateResponse(
        "credenciais_form.html",
        {"request": request, "estado": "sucesso", "automacao": automacao},
    )
