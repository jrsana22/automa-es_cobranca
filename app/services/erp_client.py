"""
HTTP Client genérico para ERPs.
Base class que faz login via HTTP e mantém sessão com cookies/tokens.
Subclasses implementam a lógica específica de cada ERP.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from io import BytesIO

import requests
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class ExportResult:
    """Resultado da exportação do ERP."""
    dataframe: pd.DataFrame
    total_registros: int
    log: str


class BaseERPClient(ABC):
    """Base class para clients de ERP. Cada ERP implementa suas próprias rotas."""

    def __init__(self, base_url: str, login: str, senha: str):
        self.base_url = base_url.rstrip("/")
        self.erp_login = login
        self.erp_senha = senha
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    @abstractmethod
    def login(self) -> bool:
        """Faz login no ERP. Retorna True se sucesso."""
        ...

    @abstractmethod
    def exportar_inadimplencia(self) -> ExportResult:
        """Navega até o relatório de inadimplência e faz download do XLSX."""
        ...

    def close(self):
        self.session.close()