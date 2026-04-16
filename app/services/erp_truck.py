"""
Client específico para o ERP APVS Truck.
Outro link e outro caminho de navegação.

NOTA: Os endpoints e parâmetros exatos serão preenchidos
quando o usuário passar o passo a passo da APVS Truck.
"""

import logging

from app.services.erp_client import BaseERPClient, ExportResult

logger = logging.getLogger(__name__)


class PVSTruckClient(BaseERPClient):
    """Client HTTP para o ERP APVS Truck."""

    def __init__(self, base_url: str, login: str, senha: str):
        super().__init__(base_url, login, senha)

    def login(self) -> bool:
        """
        Faz login no ERP APVS Truck via HTTP.
        TODO: Implementar após receber o passo a passo do usuário.
        """
        logger.info(f"Fazendo login em {self.base_url} (APVS Truck)")
        # TODO: Implementar fluxo de login específico da APVS Truck
        raise NotImplementedError("Login APVS Truck ainda não implementado")

    def exportar_inadimplencia(self) -> ExportResult:
        """
        Navega até o relatório de inadimplência e faz download do XLSX.
        TODO: Implementar após receber o passo a passo do usuário.
        """
        logger.info("Iniciando exportação de inadimplência (APVS Truck)")
        # TODO: Implementar fluxo de exportação específico da APVS Truck
        raise NotImplementedError("Exportação APVS Truck ainda não implementada")