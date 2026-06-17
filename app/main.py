import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.database import init_db, SessionLocal
from app.routers import dashboard, api, executions
from app.routers import saude as saude_router
from app.routers import formulario as formulario_router
from app.routers import relatorio as relatorio_router
from app.auth import router as auth_router
from app.migrate_db import migrate_add_dias_semana, migrate_add_fluxo_campos, migrate_add_automacao_runs, migrate_fix_vencendo_hoje_formulario, migrate_add_form_tokens, migrate_add_relatorio_capitao
from app.migrate_multi_erp import migrate_multi_erp
from app.migrate_truck_situacao import migrate_truck_situacao
from app.scheduler import iniciar_scheduler, iniciar_watchdog, scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    migrate_add_dias_semana()
    migrate_add_fluxo_campos()
    migrate_fix_vencendo_hoje_formulario()
    migrate_multi_erp()
    migrate_truck_situacao()
    migrate_add_automacao_runs()
    migrate_add_form_tokens()
    migrate_add_relatorio_capitao()
    db = SessionLocal()
    try:
        iniciar_scheduler(db)
        iniciar_watchdog()
    finally:
        db.close()
    yield
    if scheduler.running:
        scheduler.shutdown(wait=False)


app = FastAPI(title="Automação Cobrança", lifespan=lifespan)

app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

app.include_router(auth_router)
app.include_router(dashboard.router)
app.include_router(api.router, prefix="/api")
app.include_router(executions.router, prefix="/api")
app.include_router(saude_router.router)
app.include_router(formulario_router.router)
app.include_router(relatorio_router.router)