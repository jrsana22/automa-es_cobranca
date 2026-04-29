from fastapi import APIRouter, Request, Depends
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Automacao, AutomacaoRun
from app.scheduler import scheduler

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/saude", response_class=HTMLResponse)
def saude(request: Request, db: Session = Depends(get_db)):
    jobs = []
    for job in scheduler.get_jobs():
        next_run = job.next_run_time
        jobs.append({
            "id": job.id,
            "name": job.name,
            "next_run": next_run.strftime("%d/%m/%Y %H:%M") if next_run else "—",
        })

    runs = (
        db.query(AutomacaoRun, Automacao.nome)
        .join(Automacao, AutomacaoRun.automacao_id == Automacao.id)
        .order_by(AutomacaoRun.data.desc())
        .limit(50)
        .all()
    )

    runs_list = []
    for run, nome in runs:
        runs_list.append({
            "id": run.id,
            "automacao_nome": nome,
            "data": run.data.strftime("%d/%m/%Y %H:%M"),
            "status": run.status,
            "agendado": run.agendado,
            "registros_encontrados": run.registros_encontrados,
            "registros_filtrados": run.registros_filtrados,
            "duracao": run.duracao_segundos,
            "log_completo": run.log_completo or "",
        })

    return templates.TemplateResponse("saude.html", {
        "request": request,
        "scheduler_running": scheduler.running,
        "jobs": jobs,
        "runs": runs_list,
    })
