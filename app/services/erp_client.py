"""
HTTP Client genérico para ERPs.
Base class que faz login via HTTP e mantém sessão com cookies/tokens.
Subclasses implementam a lógica específica de cada ERP.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import timedelta
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

    REQUEST_TIMEOUT = 90  # segundos — evita threads presas por conexões penduradas

    def __init__(self, base_url: str, login: str, senha: str):
        self.base_url = base_url.rstrip("/")
        self.erp_login = login
        self.erp_senha = senha
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
        # Injetar timeout padrão em todos os requests sem precisar mudar cada chamada
        _orig_request = self.session.request
        _timeout = self.REQUEST_TIMEOUT
        def _request_with_timeout(*args, **kwargs):
            kwargs.setdefault("timeout", _timeout)
            return _orig_request(*args, **kwargs)
        self.session.request = _request_with_timeout

    @abstractmethod
    def login(self) -> bool:
        """Faz login no ERP. Retorna True se sucesso."""
        ...

    @abstractmethod
    def exportar_inadimplencia(
        self,
        id_formulario: str = "127000007",
        id_situacao: str = "2",
        dt_inicial: str = "",
        dt_final: str = "",
    ) -> ExportResult:
        """Navega até o relatório e faz download do XLSX com filtros."""
        ...

    def exportar_form_008(self, dt_ini, dt_fim) -> ExportResult:
        """
        Exporta form 127000008 (Pré-boleto), dividindo por mês se o período cruzar virada de mês.
        dt_ini / dt_fim: objetos date ou datetime.
        """
        from datetime import date as _date
        dt_ini = dt_ini.date() if hasattr(dt_ini, "date") else dt_ini
        dt_fim = dt_fim.date() if hasattr(dt_fim, "date") else dt_fim

        dfs = []
        cur = dt_ini
        while cur <= dt_fim:
            if cur.month == 12:
                month_end = _date(cur.year + 1, 1, 1) - timedelta(days=1)
            else:
                month_end = _date(cur.year, cur.month + 1, 1) - timedelta(days=1)
            chunk_end = min(month_end, dt_fim)
            r = self.exportar_inadimplencia(
                id_formulario="127000008",
                id_situacao="",
                dt_inicial=cur.strftime("%d/%m/%Y"),
                dt_final=chunk_end.strftime("%d/%m/%Y"),
            )
            dfs.append(r.dataframe)
            cur = chunk_end + timedelta(days=1)

        combined = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        return ExportResult(dataframe=combined, total_registros=len(combined), log=f"form 008: {len(combined)} registros")

    def close(self):
        self.session.close()