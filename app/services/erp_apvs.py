"""
Client específico para o ERP APVS (erp.apvs.com.br).
ASP.NET WebForms — login via POST, navegação por formulário e download XLSX.

Fluxo real (mapeado via reverse-engineering):
1. GET /WebClient.aspx → página "Obtendo TimeZone" (coleta fingerprint + timezone via AJAX)
2. POST /default.aspx/GravarVisitorID → grava fingerprint (visitorId vira cookie I4ProEngine)
3. POST /default.aspx/GravarTimezone → grava timezone
4. GET /Default.aspx? → página de login com __RequestVerificationToken
5. POST /Default.aspx com cd_usuario + nm_senha + token → cookie I4ProEngine atualizado
6. GET /Default.aspx?eng_idtela=127000653&eng_idmenu=127000051&eng_idmodulo=127&eng_detalhe=s
   → carrega o iframe com formulário "Auto Impressão de 2ª Via"
7. POST /Excel.aspx com todos os campos do formulário → retorna XLSX direto
   (Content-Disposition: attachment;filename=XXXXX.xlsx)
"""

import json
import logging
import uuid
from io import BytesIO
from typing import Optional
from urllib.parse import urlencode

import pandas as pd
import requests
from bs4 import BeautifulSoup

from app.services.erp_client import BaseERPClient, ExportResult

logger = logging.getLogger(__name__)

# Constantes do ERP APVS
ID_TELA = "127000653"
ID_MENU = "127000051"
ID_MODULO = "127"
ID_FORMULARIO_INADIMPLENCIA = "127000007"
ID_FORMULARIO_PREBOLETO = "127000008"
ID_SITUACAO_COBRANCA = "2"

# Fingerprint fixo para o script (cada instância do client gera um único)
_VISITOR_ID = f"fp_auto_{uuid.uuid4().hex[:12]}"


class APVSClient(BaseERPClient):
    """Client HTTP para o ERP APVS (ASP.NET WebForms)."""

    def __init__(self, base_url: str, login: str, senha: str):
        super().__init__(base_url, login, senha)
        self._eng_token: Optional[str] = None
        self._eng_chk: Optional[str] = None
        self._eng_chkch: Optional[str] = None
        self._eng_sessao_aberta: Optional[str] = None
        self._cd_papel: Optional[str] = None
        self._cd_empresa: Optional[str] = None
        self._nr_versao: Optional[str] = None

    def _extrair_hidden_fields(self, html: str) -> dict:
        """Extrai campos hidden do ASP.NET WebForms."""
        soup = BeautifulSoup(html, "html.parser")
        fields = {}
        for inp in soup.find_all("input", {"type": "hidden"}):
            name = inp.get("name")
            if name:
                fields[name] = inp.get("value", "")
        return fields

    def login(self) -> bool:
        """
        Faz login no ERP APVS via HTTP.
        Fluxo completo: fingerprint → timezone → token → credenciais.
        """
        logger.info(f"Fazendo login em {self.base_url}")

        # Step 1: GET WebClient.aspx (inicializa sessão + cookies)
        resp = self.session.get(f"{self.base_url}/WebClient.aspx")
        resp.raise_for_status()

        # Step 2: POST GravarVisitorID (fingerprint → vira cookie I4ProEngine)
        visitor_data = json.dumps({"visitorId": _VISITOR_ID})
        resp = self.session.post(
            f"{self.base_url}/default.aspx/GravarVisitorID",
            data=visitor_data,
            headers={"Content-Type": "application/json; charset=utf-8"},
        )
        resp.raise_for_status()
        logger.info(f"Fingerprint gravado: {_VISITOR_ID}")

        # Step 3: POST GravarTimezone
        tz_data = json.dumps({"timezone": "-0300"})
        resp = self.session.post(
            f"{self.base_url}/default.aspx/GravarTimezone",
            data=tz_data,
            headers={"Content-Type": "application/json; charset=utf-8"},
        )
        resp.raise_for_status()

        # Step 4: GET página de login (agora tem __RequestVerificationToken no HTML)
        resp = self.session.get(f"{self.base_url}/Default.aspx?")
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        token_input = soup.find("input", {"name": "__RequestVerificationToken"})
        verification_token = token_input.get("value", "") if token_input else ""

        if not verification_token:
            logger.error("Login falhou — __RequestVerificationToken não encontrado")
            return False

        logger.info(f"Token de verificação obtido (len={len(verification_token)})")

        # Step 5: POST com credenciais
        login_data = {
            "__RequestVerificationToken": verification_token,
            "cd_usuario": self.erp_login,
            "nm_senha": self.erp_senha,
            "eng_DataAction": "login",
            "eng_ididioma": "0",
            "eng_trocaidioma": "0",
            "eng_timezone": "",
            "idiomaID": "0",
            "eng_token": "",
        }

        resp = self.session.post(
            f"{self.base_url}/Default.aspx",
            data=login_data,
            allow_redirects=True,
        )

        # Verificar se login foi bem sucedido
        # O cookie I4ProEngine é criado no Step 2 (GravarVisitorID), antes do login,
        # então não serve como indicador de autenticação.
        # A verificação real é: se a resposta continua na página de login, as credenciais falharam.
        if "[Login]" in resp.text or "cd_usuario" in resp.text and "nm_senha" in resp.text and resp.url.rstrip("/").endswith("Default.aspx"):
            logger.error("Login falhou — credenciais recusadas pelo ERP")
            return False

        if "WebClient.aspx" in resp.url:
            logger.error("Login falhou — redirecionado de volta para login")
            return False

        logger.info("Login realizado com sucesso")
        return True

    def _carregar_tela_relatorio(self) -> dict:
        """
        Carrega a tela "Auto Impressão de 2ª Via" dentro do iframe.
        Captura os campos hidden necessários para o POST de exportação.
        """
        params = {
            "eng_idtela": ID_TELA,
            "eng_idmenu": ID_MENU,
            "eng_idmodulo": ID_MODULO,
            "eng_detalhe": "s",
        }
        url = f"{self.base_url}/Default.aspx?{urlencode(params)}"

        logger.info(f"Carregando tela de relatório: {url}")
        resp = self.session.get(url)
        resp.raise_for_status()

        # Extrair todos os campos hidden do formulário
        hidden_fields = self._extrair_hidden_fields(resp.text)

        # Salvar campos importantes para o POST de exportação
        self._eng_token = hidden_fields.get("eng_token", "")
        self._eng_chk = hidden_fields.get("eng_chk", "")
        self._eng_chkch = hidden_fields.get("eng_chkch", "")
        self._eng_sessao_aberta = hidden_fields.get("eng_sessao_aberta", "")
        self._cd_papel = hidden_fields.get("cd_papel", "")
        self._cd_empresa = hidden_fields.get("cd_empresa", "")
        self._nr_versao = hidden_fields.get("eng_nrversao", "")

        logger.info(f"Tela carregada — eng_token={self._eng_token[:20]}... cd_empresa={self._cd_empresa}")
        return hidden_fields

    def exportar_inadimplencia(
        self,
        id_formulario: str = ID_FORMULARIO_INADIMPLENCIA,
        id_situacao: str = ID_SITUACAO_COBRANCA,
        dt_inicial: str = "",
        dt_final: str = "",
    ) -> ExportResult:
        """
        Exporta relatório do ERP como XLSX.
        Args:
            id_formulario: "127000007" (Inadimplência) ou "127000008" (Pré Boleto)
            id_situacao: tipo de inadimplência ("2" = Cobrança, vazio para Pré Boleto)
            dt_inicial: data inicial do filtro (DD/MM/AAAA), vazio = sem filtro
            dt_final: data final do filtro (DD/MM/AAAA), vazio = sem filtro
        """
        logger.info(f"Exportando formulário {id_formulario}, situação={id_situacao}, período={dt_inicial} a {dt_final}")

        # Step 1: Carregar a tela do relatório para capturar tokens
        hidden_fields = self._carregar_tela_relatorio()

        # Step 2: Montar o POST para Excel.aspx
        export_data = {
            # Campos hidden do ASP.NET
            "eng_token": self._eng_token,
            "eng_sessao_aberta": self._eng_sessao_aberta,
            "eng_formmode": "ficha",
            "eng_DataAction": "",
            "eng_idmodulo": ID_MODULO,
            "eng_idtela": ID_TELA,
            "eng_nrversao": self._nr_versao,
            "eng_pagina": "1",
            "eng_registro": "1",
            "eng_filtropadrao": "",
            "eng_chk": self._eng_chk,
            "eng_filtro": "",
            "eng_idrelatorio": id_formulario,
            "eng_acao": "",
            "eng_contenttype": "",
            "eng_detalhe": "s",
            "eng_lookup": "",
            "eng_lookupchave": "",
            "eng_lookupvalor": "",
            "eng_lookuplista": "",
            "eng_lookupaux": "0",
            "eng_refreshaux": "0",
            "eng_query": "",
            "eng_orderby": "",
            "eng_idmenu": ID_MENU,
            "eng_historico": " Auto Impressão de 2ª Via",
            "eng_detalheaux": "",
            "eng_tema": "#004182",
            "eng_idioma": "207",
            "eng_idacao": "",
            "eng_selpagina": "",
            "eng_edicao": "",
            "eng_atualiza_opener": "",
            "eng_indiceacaoregistro": "",
            "eng_indicerelatorioregistro": "",
            "ecm_chavedominio": "",
            "wex_instancia": "",
            "eng_tipo_excel": "xlsx",
            "eng_download_token": "",
            "eng_formatoData": "dd/MM/yyyy",
            "tela_leitura": "",
            "eng_ecm_naoexibeinserir": "",
            "eng_erroaux": "0",
            "eng_dataactionaux": "ModoFicha",
            "eng_validade_eis": "0",
            "eng_chave": "id_processo=0",
            "eng_chkch": self._eng_chkch,
            # Campos do formulário
            "id_formulario": id_formulario,
            "id_situacao_inadimplente": id_situacao,
            "id_ano_consulta": "",
            "id_mes_consulta": "",
            "id_processo": "0",
            "cd_papel": self._cd_papel,
            "cd_usuario_login": self.erp_login,
            "cd_empresa": self._cd_empresa,
            # Campos lookup
            "txtid_pessoa_diretor_estado": "[Não selecionado]",
            "id_pessoa_diretor_estado": "",
            "lkpid_pessoa_diretor_estado": "[Não selecionado]",
            "txtid_pessoa_gerente_regional": "[Não selecionado]",
            "id_pessoa_gerente_regional": "",
            "lkpid_pessoa_gerente_regional": "[Não selecionado]",
            "txtid_regional": "[Não selecionado]",
            "id_regional": "",
            "lkpid_regional": "[Não selecionado]",
            "txtid_pessoa_consultor_lider": "[Não selecionado]",
            "id_pessoa_consultor_lider": "",
            "lkpid_pessoa_consultor_lider": "[Não selecionado]",
            "txtid_pessoa_consultor": "[Não selecionado]",
            "id_pessoa_consultor": "",
            "lkpid_pessoa_consultor": "[Não selecionado]",
            "txtid_identificacao_externa": "[Não selecionado]",
            "id_identificacao_externa": "",
            "lkpid_identificacao_externa": "[Não selecionado]",
            "txtid_pessoa": "[Não selecionado]",
            "id_pessoa": "",
            "lkpid_pessoa": "[Não selecionado]",
            # Outros campos
            "idsituacao": "",
            "id_mes_pendencia": "",
            "nr_ano_pendencia": str(pd.Timestamp.now().year),
            "dv_ativo": "",
            # Filtro de datas por formulário
            f"dt_inicial_id{id_formulario}": dt_inicial,
            f"dt_final_id{id_formulario}": dt_final,
            # Datas genéricas
            "dt_inicio": "",
            "dt_fim": "",
            "dt_pagto": "",
            "dt_agendamento_inicio": "",
            "dt_agendamento_fim": "",
            "dt_movimento": "",
            "dt_fup_inicio": "",
            "dt_fup_fim": "",
            "dt_inicial": "",
            "dt_final": "",
        }

        # Step 3: POST para Excel.aspx
        logger.info("Enviando POST para Excel.aspx...")
        resp = self.session.post(
            f"{self.base_url}/Excel.aspx",
            data=export_data,
        )
        resp.raise_for_status()

        # Step 4: Verificar se a resposta é XLSX
        content_type = resp.headers.get("Content-Type", "")
        content_disposition = resp.headers.get("Content-Disposition", "")

        if "spreadsheet" in content_type or "octet-stream" in content_type or ".xlsx" in content_disposition:
            df = pd.read_excel(BytesIO(resp.content), engine="openpyxl")
            logger.info(f"Exportação concluída: {len(df)} registros, colunas: {list(df.columns)}")
            return ExportResult(
                dataframe=df,
                total_registros=len(df),
                log=f"Exportação concluída: {len(df)} registros",
            )
        else:
            raise Exception(
                f"Resposta inesperada do ERP. Content-Type: {content_type}, "
                f"Content-Disposition: {content_disposition}"
            )