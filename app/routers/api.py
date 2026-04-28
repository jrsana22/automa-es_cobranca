from fastapi import APIRouter, Depends, HTTPException, Form
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.database import get_db
from app.models import Automacao, ERPConfig, Execucao, Fluxo, get_fluxos_padrao, ERP_TIPOS
from app.crypto import encrypt_password, decrypt_password
from app.routers.executions import _run_automation_bg, _running_automations
from app.scheduler import atualizar_agendamentos
from app.services.erp_factory import criar_erp_client

router = APIRouter()


def _parse_erp_forms(form_data: dict, prefix: str) -> dict:
    """Extrai dados de um ERP do formulário com prefixo (brasil_ ou truck_)."""
    return {
        "erp_tipo": form_data.get(f"{prefix}erp_tipo", ""),
        "erp_url": form_data.get(f"{prefix}erp_url", ""),
        "erp_login": form_data.get(f"{prefix}erp_login", ""),
        "erp_senha": form_data.get(f"{prefix}erp_senha", ""),
        "ativo": form_data.get(f"{prefix}ativo", "off") == "on",
        # Fluxos
        "preboleto_aba": form_data.get(f"{prefix}preboleto_aba", ""),
        "preboleto_ativo": form_data.get(f"{prefix}preboleto_ativo", "off") == "on",
        "vencendo_hoje_aba": form_data.get(f"{prefix}vencendo_hoje_aba", ""),
        "vencendo_hoje_ativo": form_data.get(f"{prefix}vencendo_hoje_ativo", "off") == "on",
        "cobranca_d1_aba": form_data.get(f"{prefix}cobranca_d1_aba", ""),
        "cobranca_d1_ativo": form_data.get(f"{prefix}cobranca_d1_ativo", "off") == "on",
        "cobranca_2_30_aba": form_data.get(f"{prefix}cobranca_2_30_aba", ""),
        "cobranca_2_30_ativo": form_data.get(f"{prefix}cobranca_2_30_ativo", "off") == "on",
        "reativacao_aba": form_data.get(f"{prefix}reativacao_aba", ""),
        "reativacao_ativo": form_data.get(f"{prefix}reativacao_ativo", "off") == "on",
    }


@router.post("/automacoes")
def criar_automacao(
    nome: str = Form(...),
    sheets_url: str = Form(...),
    coluna_vencimento: str = Form("vencimento_Parcela"),
    horario_execucao: str = Form("06:00"),
    dias_semana: str = Form("0,1,2,3,4"),
    dia_cobranca_base: int = Form(1),
    mapeamento: str = Form("{}"),
    executar_agora: bool = Form(False),
    # ERP Brasil
    brasil_erp_tipo: str = Form("apvs_brasil"),
    brasil_erp_url: str = Form(""),
    brasil_erp_login: str = Form(""),
    brasil_erp_senha: str = Form(""),
    brasil_ativo: str = Form("off"),
    brasil_preboleto_aba: str = Form("D-7 - PRÉ-BOLETO"),
    brasil_preboleto_ativo: str = Form("on"),
    brasil_vencendo_hoje_aba: str = Form("VENCIMENTO NO DIA"),
    brasil_vencendo_hoje_ativo: str = Form("on"),
    brasil_cobranca_d1_aba: str = Form("D+1 - COBRANÇA"),
    brasil_cobranca_d1_ativo: str = Form("on"),
    brasil_cobranca_2_30_aba: str = Form("COBRANÇA 2-30D"),
    brasil_cobranca_2_30_ativo: str = Form("on"),
    brasil_reativacao_aba: str = Form("REATIVAÇÃO"),
    brasil_reativacao_ativo: str = Form("on"),
    # ERP Truck
    truck_erp_tipo: str = Form("apvs_truck"),
    truck_erp_url: str = Form(""),
    truck_erp_login: str = Form(""),
    truck_erp_senha: str = Form(""),
    truck_ativo: str = Form("off"),
    truck_preboleto_aba: str = Form("TRUCK - D-7 - PRÉ-BOLETO"),
    truck_preboleto_ativo: str = Form("on"),
    truck_vencendo_hoje_aba: str = Form("TRUCK VENCIMENTO NO DIA"),
    truck_vencendo_hoje_ativo: str = Form("on"),
    truck_cobranca_d1_aba: str = Form("TRUCK - INADIMPLÊNCIA D+1"),
    truck_cobranca_d1_ativo: str = Form("on"),
    truck_cobranca_2_30_aba: str = Form("TRUCK - COBRANÇA - 2 DIAS VENCIDO"),
    truck_cobranca_2_30_ativo: str = Form("on"),
    truck_reativacao_aba: str = Form("TRUCK - REATIVAÇÃO"),
    truck_reativacao_ativo: str = Form("on"),
    db: Session = Depends(get_db),
):
    automacao = Automacao(
        nome=nome,
        sheets_url=sheets_url,
        coluna_vencimento=coluna_vencimento,
        horario_execucao=horario_execucao,
        dias_semana=dias_semana,
        dia_cobranca_base=dia_cobranca_base,
        mapeamento_json=mapeamento if mapeamento else "{}",
    )
    db.add(automacao)
    db.commit()
    db.refresh(automacao)

    # Criar ERP configs + fluxos
    erps_data = [
        {
            "tipo": "apvs_brasil",
            "url": brasil_erp_url,
            "login": brasil_erp_login,
            "senha": brasil_erp_senha,
            "ativo": brasil_ativo == "on",
            "fluxos_abas": {
                "preboleto": {"aba": brasil_preboleto_aba, "ativo": brasil_preboleto_ativo == "on"},
                "vencendo_hoje": {"aba": brasil_vencendo_hoje_aba, "ativo": brasil_vencendo_hoje_ativo == "on"},
                "cobranca_d1": {"aba": brasil_cobranca_d1_aba, "ativo": brasil_cobranca_d1_ativo == "on"},
                "cobranca_2_30": {"aba": brasil_cobranca_2_30_aba, "ativo": brasil_cobranca_2_30_ativo == "on"},
                "reativacao": {"aba": brasil_reativacao_aba, "ativo": brasil_reativacao_ativo == "on"},
            },
        },
        {
            "tipo": "apvs_truck",
            "url": truck_erp_url,
            "login": truck_erp_login,
            "senha": truck_erp_senha,
            "ativo": truck_ativo == "on",
            "fluxos_abas": {
                "preboleto": {"aba": truck_preboleto_aba, "ativo": truck_preboleto_ativo == "on"},
                "vencendo_hoje": {"aba": truck_vencendo_hoje_aba, "ativo": truck_vencendo_hoje_ativo == "on"},
                "cobranca_d1": {"aba": truck_cobranca_d1_aba, "ativo": truck_cobranca_d1_ativo == "on"},
                "cobranca_2_30": {"aba": truck_cobranca_2_30_aba, "ativo": truck_cobranca_2_30_ativo == "on"},
                "reativacao": {"aba": truck_reativacao_aba, "ativo": truck_reativacao_ativo == "on"},
            },
        },
    ]

    for erp_data in erps_data:
        # Só criar se tem URL e login preenchidos
        if not erp_data["url"] or not erp_data["login"]:
            continue

        erp_config = ERPConfig(
            automacao_id=automacao.id,
            erp_tipo=erp_data["tipo"],
            erp_url=erp_data["url"],
            erp_login=erp_data["login"],
            erp_senha=encrypt_password(erp_data["senha"]),
            ativo=erp_data["ativo"],
        )
        db.add(erp_config)
        db.commit()
        db.refresh(erp_config)

        # Criar 5 fluxos padrão
        fluxos_padrao = get_fluxos_padrao(erp_data["tipo"])
        for fluxo_padrao in fluxos_padrao:
            custom = erp_data["fluxos_abas"].get(fluxo_padrao["tipo"], {})
            fluxo = Fluxo(
                erp_config_id=erp_config.id,
                tipo=fluxo_padrao["tipo"],
                nome=fluxo_padrao["nome"],
                sheets_aba=custom.get("aba", fluxo_padrao["sheets_aba"]),
                filtro_dias_min=fluxo_padrao["filtro_dias_min"],
                filtro_dias_max=fluxo_padrao["filtro_dias_max"],
                formulario_id=fluxo_padrao["formulario_id"],
                situacao_id=fluxo_padrao["situacao_id"],
                ativo=custom.get("ativo", True),
            )
            db.add(fluxo)

    db.commit()

    # Executar imediatamente se solicitado
    if executar_agora:
        import threading
        thread = threading.Thread(
            target=_run_automation_bg,
            args=(automacao.id,),
            daemon=True,
        )
        thread.start()

    atualizar_agendamentos(db)
    return RedirectResponse(url="/", status_code=303)


@router.post("/automacoes/{automacao_id}")
def atualizar_automacao(
    automacao_id: int,
    nome: str = Form(...),
    sheets_url: str = Form(...),
    coluna_vencimento: str = Form("vencimento_Parcela"),
    horario_execucao: str = Form("06:00"),
    dias_semana: str = Form("0,1,2,3,4"),
    dia_cobranca_base: int = Form(1),
    ativo: bool = Form(True),
    mapeamento: str = Form("{}"),
    # ERP Brasil
    brasil_erp_url: str = Form(""),
    brasil_erp_login: str = Form(""),
    brasil_erp_senha: str = Form(""),
    brasil_ativo: str = Form("off"),
    brasil_preboleto_aba: str = Form(None),
    brasil_preboleto_ativo: str = Form("off"),
    brasil_vencendo_hoje_aba: str = Form(None),
    brasil_vencendo_hoje_ativo: str = Form("off"),
    brasil_cobranca_d1_aba: str = Form(None),
    brasil_cobranca_d1_ativo: str = Form("off"),
    brasil_cobranca_2_30_aba: str = Form(None),
    brasil_cobranca_2_30_ativo: str = Form("off"),
    brasil_reativacao_aba: str = Form(None),
    brasil_reativacao_ativo: str = Form("off"),
    # ERP Truck
    truck_erp_url: str = Form(""),
    truck_erp_login: str = Form(""),
    truck_erp_senha: str = Form(""),
    truck_ativo: str = Form("off"),
    truck_preboleto_aba: str = Form(None),
    truck_preboleto_ativo: str = Form("off"),
    truck_vencendo_hoje_aba: str = Form(None),
    truck_vencendo_hoje_ativo: str = Form("off"),
    truck_cobranca_d1_aba: str = Form(None),
    truck_cobranca_d1_ativo: str = Form("off"),
    truck_cobranca_2_30_aba: str = Form(None),
    truck_cobranca_2_30_ativo: str = Form("off"),
    truck_reativacao_aba: str = Form(None),
    truck_reativacao_ativo: str = Form("off"),
    db: Session = Depends(get_db),
):
    automacao = db.query(Automacao).filter(Automacao.id == automacao_id).first()
    if not automacao:
        raise HTTPException(status_code=404)

    automacao.nome = nome
    automacao.sheets_url = sheets_url
    automacao.coluna_vencimento = coluna_vencimento
    automacao.horario_execucao = horario_execucao
    automacao.dias_semana = dias_semana
    automacao.dia_cobranca_base = dia_cobranca_base
    automacao.ativo = ativo
    automacao.mapeamento_json = mapeamento if mapeamento else "{}"

    # Processar ERPs
    erps_data = [
        {
            "tipo": "apvs_brasil",
            "url": brasil_erp_url,
            "login": brasil_erp_login,
            "senha": brasil_erp_senha,
            "ativo": brasil_ativo == "on",
            "fluxos_abas": {
                "preboleto": {"aba": brasil_preboleto_aba, "ativo": brasil_preboleto_ativo == "on"},
                "vencendo_hoje": {"aba": brasil_vencendo_hoje_aba, "ativo": brasil_vencendo_hoje_ativo == "on"},
                "cobranca_d1": {"aba": brasil_cobranca_d1_aba, "ativo": brasil_cobranca_d1_ativo == "on"},
                "cobranca_2_30": {"aba": brasil_cobranca_2_30_aba, "ativo": brasil_cobranca_2_30_ativo == "on"},
                "reativacao": {"aba": brasil_reativacao_aba, "ativo": brasil_reativacao_ativo == "on"},
            },
        },
        {
            "tipo": "apvs_truck",
            "url": truck_erp_url,
            "login": truck_erp_login,
            "senha": truck_erp_senha,
            "ativo": truck_ativo == "on",
            "fluxos_abas": {
                "preboleto": {"aba": truck_preboleto_aba, "ativo": truck_preboleto_ativo == "on"},
                "vencendo_hoje": {"aba": truck_vencendo_hoje_aba, "ativo": truck_vencendo_hoje_ativo == "on"},
                "cobranca_d1": {"aba": truck_cobranca_d1_aba, "ativo": truck_cobranca_d1_ativo == "on"},
                "cobranca_2_30": {"aba": truck_cobranca_2_30_aba, "ativo": truck_cobranca_2_30_ativo == "on"},
                "reativacao": {"aba": truck_reativacao_aba, "ativo": truck_reativacao_ativo == "on"},
            },
        },
    ]

    for erp_data in erps_data:
        erp_tipo = erp_data["tipo"]
        # Encontrar ERP config existente ou criar novo
        erp_config = db.query(ERPConfig).filter(
            ERPConfig.automacao_id == automacao.id,
            ERPConfig.erp_tipo == erp_tipo,
        ).first()

        if not erp_data["url"]:
            # Se não tem URL e existe, remover
            if erp_config:
                db.delete(erp_config)
            continue

        if erp_config:
            # Atualizar existente
            erp_config.erp_url = erp_data["url"]
            erp_config.erp_login = erp_data["login"]
            if erp_data["senha"]:
                erp_config.erp_senha = encrypt_password(erp_data["senha"])
            erp_config.ativo = erp_data["ativo"]
        else:
            # Criar novo
            erp_config = ERPConfig(
                automacao_id=automacao.id,
                erp_tipo=erp_tipo,
                erp_url=erp_data["url"],
                erp_login=erp_data["login"],
                erp_senha=encrypt_password(erp_data["senha"]),
                ativo=erp_data["ativo"],
            )
            db.add(erp_config)
            db.commit()
            db.refresh(erp_config)

            # Criar fluxos padrão para novo ERP
            fluxos_padrao = get_fluxos_padrao(erp_tipo)
            for fluxo_padrao in fluxos_padrao:
                custom = erp_data["fluxos_abas"].get(fluxo_padrao["tipo"], {})
                fluxo = Fluxo(
                    erp_config_id=erp_config.id,
                    tipo=fluxo_padrao["tipo"],
                    nome=fluxo_padrao["nome"],
                    sheets_aba=custom.get("aba", fluxo_padrao["sheets_aba"]),
                    filtro_dias_min=fluxo_padrao["filtro_dias_min"],
                    filtro_dias_max=fluxo_padrao["filtro_dias_max"],
                    formulario_id=fluxo_padrao["formulario_id"],
                    situacao_id=fluxo_padrao["situacao_id"],
                    ativo=custom.get("ativo", True),
                )
                db.add(fluxo)

        # Atualizar fluxos existentes
        if erp_config and erp_config.id:
            for fluxo in erp_config.fluxos:
                update = erp_data["fluxos_abas"].get(fluxo.tipo, {})
                if update.get("aba"):
                    fluxo.sheets_aba = update["aba"]
                fluxo.ativo = update.get("ativo", True)

    db.commit()
    atualizar_agendamentos(db)
    return RedirectResponse(url="/", status_code=303)


@router.patch("/automacoes/{automacao_id}/toggle")
def toggle_automacao(automacao_id: int, db: Session = Depends(get_db)):
    automacao = db.query(Automacao).filter(Automacao.id == automacao_id).first()
    if not automacao:
        raise HTTPException(status_code=404)
    automacao.ativo = not automacao.ativo
    db.commit()
    atualizar_agendamentos(db)
    return {"ok": True, "ativo": automacao.ativo}


@router.patch("/automacoes/{automacao_id}/erp/{erp_config_id}/toggle")
def toggle_erp_config(automacao_id: int, erp_config_id: int, db: Session = Depends(get_db)):
    erp_config = db.query(ERPConfig).filter(
        ERPConfig.id == erp_config_id,
        ERPConfig.automacao_id == automacao_id,
    ).first()
    if not erp_config:
        raise HTTPException(status_code=404)
    erp_config.ativo = not erp_config.ativo
    db.commit()
    return {"ok": True, "ativo": erp_config.ativo}


@router.patch("/automacoes/{automacao_id}/fluxos/{fluxo_id}/toggle")
def toggle_fluxo(automacao_id: int, fluxo_id: int, db: Session = Depends(get_db)):
    fluxo = db.query(Fluxo).filter(Fluxo.id == fluxo_id).first()
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
    atualizar_agendamentos(db)
    return {"ok": True}


@router.get("/status")
def status_em_tempo_real(db: Session = Depends(get_db)):
    """
    Retorna status em tempo real: quais automações estão rodando,
    qual fluxo está rodando agora, e último status de cada fluxo.
    """
    from sqlalchemy.orm import joinedload
    from app.routers.executions import _running_automations, _running_fluxo_info

    running = list(_running_automations)

    # Buscar todas as automações com ERP configs e fluxos em 1 query
    automacoes = db.query(Automacao).options(
        joinedload(Automacao.erp_configs).joinedload(ERPConfig.fluxos)
    ).all()

    # Buscar a última execução de cada fluxo em 1 query
    from sqlalchemy import func as sqlfunc
    from app.models import Execucao

    # Subquery: max id por fluxo_id
    subq = db.query(
        Execucao.fluxo_id,
        sqlfunc.max(Execucao.id).label('max_id')
    ).filter(Execucao.fluxo_id.isnot(None)).group_by(Execucao.fluxo_id).subquery()

    # Buscar as execuções pelo id
    last_execs = db.query(Execucao).filter(
        Execucao.id.in_(db.query(subq.c.max_id))
    ).all() if db.query(subq.c.max_id).first() else []

    # Indexar por fluxo_id
    exec_by_fluxo = {e.fluxo_id: e for e in last_execs if e.fluxo_id}

    fluxos_status = {}
    for auto in automacoes:
        fluxos_status[str(auto.id)] = {}
        for erp_config in auto.erp_configs:
            for fluxo in erp_config.fluxos:
                key = f"{erp_config.erp_tipo}_{fluxo.tipo}"
                is_running_now = (
                    auto.id in _running_automations
                    and auto.id in _running_fluxo_info
                    and _running_fluxo_info[auto.id].get("fluxo") == fluxo.nome
                )
                if is_running_now:
                    fluxos_status[str(auto.id)][key] = {
                        "status": "executando",
                        "registros": 0,
                        "hora": None,
                        "erp_tipo": erp_config.erp_tipo,
                        "tipo": fluxo.tipo,
                        "nome": fluxo.nome,
                        "erp_nome": ERP_TIPOS.get(erp_config.erp_tipo, erp_config.erp_tipo),
                    }
                else:
                    ultima_exec = exec_by_fluxo.get(fluxo.id)
                    if ultima_exec:
                        fluxos_status[str(auto.id)][key] = {
                            "status": ultima_exec.status,
                            "registros": ultima_exec.registros_filtrados,
                            "hora": ultima_exec.data.strftime("%d/%m %H:%M"),
                            "erp_tipo": erp_config.erp_tipo,
                            "tipo": fluxo.tipo,
                            "nome": fluxo.nome,
                            "erp_nome": ERP_TIPOS.get(erp_config.erp_tipo, erp_config.erp_tipo),
                        }
                    else:
                        fluxos_status[str(auto.id)][key] = {
                            "status": "pendente",
                            "registros": 0,
                            "hora": None,
                            "erp_tipo": erp_config.erp_tipo,
                            "tipo": fluxo.tipo,
                            "nome": fluxo.nome,
                            "erp_nome": ERP_TIPOS.get(erp_config.erp_tipo, erp_config.erp_tipo),
                        }

    # Info do que está rodando agora
    running_info = {}
    for auto_id, info in _running_fluxo_info.items():
        running_info[str(auto_id)] = info

    return {
        "running": running,
        "running_info": running_info,
        "fluxos_status": fluxos_status,
    }


@router.post("/test-login/{erp_config_id}")
def test_login(erp_config_id: int, db: Session = Depends(get_db)):
    """Testa login do ERP sem executar automação."""
    erp_config = db.query(ERPConfig).filter(ERPConfig.id == erp_config_id).first()
    if not erp_config:
        raise HTTPException(status_code=404, detail="ERP config não encontrado")

    senha = decrypt_password(erp_config.erp_senha)
    client = criar_erp_client(erp_config, senha)
    try:
        ok = client.login()
        client.close()
        return {"ok": ok, "message": "Login realizado com sucesso!" if ok else "Credenciais inválidas."}
    except Exception as e:
        return {"ok": False, "message": f"Erro: {str(e)}"}