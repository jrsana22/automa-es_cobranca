from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.database import init_db
from app.routers import dashboard, api, executions
from app.migrate_db import migrate_add_dias_semana, migrate_add_fluxo_campos


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    migrate_add_dias_semana()
    migrate_add_fluxo_campos()
    yield


app = FastAPI(title="Automação Cobrança", lifespan=lifespan)

app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

app.include_router(dashboard.router)
app.include_router(api.router, prefix="/api")
app.include_router(executions.router, prefix="/api")