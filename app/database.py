import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase

from app.config import settings


def _build_engine_url() -> str:
    """
    Constroi a URL do banco de dados.
    - Se DATABASE_URL começa com postgres://, converte para postgresql:// (SQLAlchemy requer isso)
    - Se começa com sqlite://, usa SQLite (dev)
    - Caso contrário, usa o valor diretamente
    """
    url = settings.DATABASE_URL

    # Heroku/PostgreSQL: converter postgres:// para postgresql://
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)

    return url


engine = create_engine(
    _build_engine_url(),
    connect_args={"check_same_thread": False} if settings.DATABASE_URL.startswith("sqlite") else {},
    pool_pre_ping=True,
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    Base.metadata.create_all(bind=engine)