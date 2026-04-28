from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.database import init_db, SessionLocal
from app.routers import dashboard, api, executions
from app.migrate_db import migrate_add_dias_semana, migrate_add_fluxo_campos
from app.migrate_multi_erp import migrate_multi_erp
from app.scheduler import iniciar_scheduler, scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    migrate_add_dias_semana()
    migrate_add_fluxo_campos()
    migrate_multi_erp()
    db = SessionLocal()
    try:
        iniciar_scheduler(db)
    finally:
        db.close()
    yield
    if scheduler.running:
        scheduler.shutdown(wait=False)


app = FastAPI(title="Automação Cobrança", lifespan=lifespan)

app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

app.include_router(dashboard.router)
app.include_router(api.router, prefix="/api")
app.include_router(executions.router, prefix="/api")