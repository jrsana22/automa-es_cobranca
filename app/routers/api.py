from fastapi import APIRouter, Depends, HTTPException, Form
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Automacao, Fluxo, FLUXOS_PADRAO
from app.crypto import encrypt_password, decrypt_password
from app.routers.executions import _executar_automacao

router = APIRouter()


@router.post("/automacoes")
def criar_automacao(
    nome: str = Form(...),
    erp_url: str = Form(...),
    erp_login: str = Form(...),
    erp_senha: str = Form(...),
    erp_tipo: str = Form("apvs_brasil"),
    sheets_url: str = Form(...),
    coluna_vencimento: str = Form("vencimento_Parcela"),
    horario_execucao: str = Form("06:00"),
    dia_cobranca_base: int = Form(1),
    mapeamento: str = Form("{}"),
    # Fluxos (nomes das abas e ativos)
    # Pré-boleto
    preboleto_aba: str = Form("Pré-boleto"),
    preboleto_ativo: bool = Form(True),
    # Vencendo Hoje
    vencendo_hoje_aba: str = Form("Vencendo Hoje"),
    vencendo_hoje_ativo: bool = Form(True),
    # Cobrança D+1
    cobranca_d1_aba: str = Form("D+1"),
    cobranca_d1_ativo: bool = Form(True),
    # Cobrança 2-30D
    cobranca_2_30_aba: str = Form("Cobrança 2-30D"),
    cobranca_2_30_ativo: bool = Form(True),
    # Reativação
    reativacao_aba: str = Form("Reativação"),
    reativacao_ativo: bool = Form(True),
    # Executar imediatamente?
    executar_agora: bool = Form(False),
    db: Session = Depends(get_db),
):
    automacao = Automacao(
        nome=nome,
        erp_url=erp_url,
        erp_login=erp_login,
        erp_senha=encrypt_password(erp_senha),
        erp_tipo=erp_tipo,
        sheets_url=sheets_url,
        coluna_vencimento=coluna_vencimento,
        horario_execucao=horario_execucao,
        dia_cobranca_base=dia_cobranca_base,
        mapeamento_json=mapeamento if mapeamento else "{}",
    )
    db.add(automacao)
    db.commit()
    db.refresh(automacao)

    # Criar os 5 fluxos padrão
    fluxos_custom = {
        "preboleto": {"sheets_aba": preboleto_aba, "ativo": preboleto_ativo},
        "vencendo_hoje": {"sheets_aba": vencendo_hoje_aba, "ativo": vencendo_hoje_ativo},
        "cobranca_d1": {"sheets_aba": cobranca_d1_aba, "ativo": cobranca_d1_ativo},
        "cobranca_2_30": {"sheets_aba": cobranca_2_30_aba, "ativo": cobranca_2_30_ativo},
        "reativacao": {"sheets_aba": reativacao_aba, "ativo": reativacao_ativo},
    }

    for fluxo_padrao in FLUXOS_PADRAO:
        custom = fluxos_custom.get(fluxo_padrao["tipo"], {})
        fluxo = Fluxo(
            automacao_id=automacao.id,
            tipo=fluxo_padrao["tipo"],
            nome=fluxo_padrao["nome"],
            sheets_aba=custom.get("sheets_aba", fluxo_padrao["sheets_aba"]),
            filtro_dias_min=fluxo_padrao["filtro_dias_min"],
            filtro_dias_max=fluxo_padrao["filtro_dias_max"],
            ativo=custom.get("ativo", True),
        )
        db.add(fluxo)

    db.commit()

    # Executar imediatamente se solicitado
    if executar_agora:
        _executar_automacao(automacao.id, db)

    return RedirectResponse(url="/", status_code=303)


@router.post("/automacoes/{automacao_id}")
def atualizar_automacao(
    automacao_id: int,
    nome: str = Form(...),
    erp_url: str = Form(...),
    erp_login: str = Form(...),
    erp_senha: str = Form(None),
    erp_tipo: str = Form("apvs_brasil"),
    sheets_url: str = Form(...),
    coluna_vencimento: str = Form("vencimento_Parcela"),
    horario_execucao: str = Form("06:00"),
    dia_cobranca_base: int = Form(1),
    ativo: bool = Form(True),
    mapeamento: str = Form("{}"),
    # Fluxos
    preboleto_aba: str = Form(None),
    preboleto_ativo: bool = Form(False),
    vencendo_hoje_aba: str = Form(None),
    vencendo_hoje_ativo: bool = Form(False),
    cobranca_d1_aba: str = Form(None),
    cobranca_d1_ativo: bool = Form(False),
    cobranca_2_30_aba: str = Form(None),
    cobranca_2_30_ativo: bool = Form(False),
    reativacao_aba: str = Form(None),
    reativacao_ativo: bool = Form(False),
    db: Session = Depends(get_db),
):
    automacao = db.query(Automacao).filter(Automacao.id == automacao_id).first()
    if not automacao:
        raise HTTPException(status_code=404)

    automacao.nome = nome
    automacao.erp_url = erp_url
    automacao.erp_login = erp_login
    if erp_senha:
        automacao.erp_senha = encrypt_password(erp_senha)
    automacao.erp_tipo = erp_tipo
    automacao.sheets_url = sheets_url
    automacao.coluna_vencimento = coluna_vencimento
    automacao.horario_execucao = horario_execucao
    automacao.dia_cobranca_base = dia_cobranca_base
    automacao.ativo = ativo
    automacao.mapeamento_json = mapeamento if mapeamento else "{}"

    # Atualizar fluxos
    fluxos_updates = {
        "preboleto": {"sheets_aba": preboleto_aba, "ativo": preboleto_ativo},
        "vencendo_hoje": {"sheets_aba": vencendo_hoje_aba, "ativo": vencendo_hoje_ativo},
        "cobranca_d1": {"sheets_aba": cobranca_d1_aba, "ativo": cobranca_d1_ativo},
        "cobranca_2_30": {"sheets_aba": cobranca_2_30_aba, "ativo": cobranca_2_30_ativo},
        "reativacao": {"sheets_aba": reativacao_aba, "ativo": reativacao_ativo},
    }

    for fluxo in automacao.fluxos:
        update = fluxos_updates.get(fluxo.tipo, {})
        if update.get("sheets_aba"):
            fluxo.sheets_aba = update["sheets_aba"]
        if update.get("ativo") is not None:
            fluxo.ativo = update["ativo"]

    db.commit()
    return RedirectResponse(url="/", status_code=303)


@router.patch("/automacoes/{automacao_id}/toggle")
def toggle_automacao(automacao_id: int, db: Session = Depends(get_db)):
    automacao = db.query(Automacao).filter(Automacao.id == automacao_id).first()
    if not automacao:
        raise HTTPException(status_code=404)
    automacao.ativo = not automacao.ativo
    db.commit()
    return {"ok": True, "ativo": automacao.ativo}


@router.patch("/automacoes/{automacao_id}/fluxos/{fluxo_id}/toggle")
def toggle_fluxo(automacao_id: int, fluxo_id: int, db: Session = Depends(get_db)):
    fluxo = db.query(Fluxo).filter(
        Fluxo.id == fluxo_id,
        Fluxo.automacao_id == automacao_id,
    ).first()
    if not fluxo:
        raise HTTPException(status_code=404)
    fluxo.ativo = not fluxo.ativo
    db.commit()
    return {"ok": True, "ativo": fluxo.ativo}


@router.delete("/automacoes/{automacao_id}")
def deletar_automacao(automacao_id: int, db: Session = Depends(get_db)):
    automacao = db.query(Automacao).filter(Automacao.id == automacao_id).first()
    if not automacao:
        raise HTTPException(status_code=404)
    db.delete(automacao)
    db.commit()
    return {"ok": True}