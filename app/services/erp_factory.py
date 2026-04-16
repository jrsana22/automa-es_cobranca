"""
Factory para criar o client ERP correto baseado no tipo.
Tipos suportados: apvs_brasil, apvs_truck
"""

from app.models import Automacao
from app.services.erp_apvs import APVSClient
from app.services.erp_truck import PVSTruckClient
from app.services.erp_client import BaseERPClient


ERP_CLIENTS = {
    "apvs_brasil": APVSClient,
    "apvs_truck": PVSTruckClient,
}


def criar_erp_client(automacao: Automacao, senha_decriptada: str) -> BaseERPClient:
    """
    Cria o client ERP correto para a automação.
    """
    client_class = ERP_CLIENTS.get(automacao.erp_tipo)
    if not client_class:
        raise ValueError(f"Tipo de ERP não suportado: {automacao.erp_tipo}")

    return client_class(
        base_url=automacao.erp_url,
        login=automacao.erp_login,
        senha=senha_decriptada,
    )