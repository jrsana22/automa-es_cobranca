import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    SECRET_KEY: str = os.getenv("SECRET_KEY", "dev-key-change-in-production")
    ENCRYPTION_KEY: str = os.getenv("ENCRYPTION_KEY", "dev-encryption-key-32bytes!!")
    # SQLite para dev, PostgreSQL para produção
    # Ex SQLite: sqlite:///./automacao.db
    # Ex PostgreSQL: postgresql://user:pass@host:5432/dbname
    DATABASE_URL: str = os.getenv("DATABASE_URL", f"sqlite:///{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/automacao.db")
    GOOGLE_CREDENTIALS_PATH: str = os.getenv(
        "GOOGLE_CREDENTIALS_PATH", os.path.expanduser("~/credentials.json")
    )
    APP_HOST: str = os.getenv("APP_HOST", "0.0.0.0")
    APP_PORT: int = int(os.getenv("APP_PORT", "8000"))
    N8N_WEBHOOK_URL: str = os.getenv("N8N_WEBHOOK_URL", "")
    APP_BASE_URL: str = os.getenv("APP_BASE_URL", "http://178.105.74.196:8000")
    APVS_BRASIL_URL: str = os.getenv("APVS_BRASIL_URL", "https://erp.apvs.com.br")
    APVS_TRUCK_URL: str = os.getenv("APVS_TRUCK_URL", "https://truck.apvs.com.br")


settings = Settings()