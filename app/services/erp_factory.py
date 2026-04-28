"""
Factory para criar o client ERP correto baseado no tipo.
Tipos suportados: apvs_brasil, apvs_truck
"""

from app.models import ERPConfig
from app.services.erp_apvs import APVSClient
from app.services.erp_truck import PVSTruckClient
from app.services.erp_client import BaseERPClient


ERP_CLIENTS = {
    "apvs_brasil": APVSClient,
    "apvs_truck": PVSTruckClient,
}


def criar_erp_client(erp_config: ERPConfig, senha_decriptada: str) -> BaseERPClient:
    """
    Cria o client ERP correto para a configuração.
    """
    client_class = ERP_CLIENTS.get(erp_config.erp_tipo)
    if not client_class:
        raise ValueError(f"Tipo de ERP não suportado: {erp_config.erp_tipo}")

    return client_class(
        base_url=erp_config.erp_url,
        login=erp_config.erp_login,
        senha=senha_decriptada,
    )