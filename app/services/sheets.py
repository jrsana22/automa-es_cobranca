"""
Google Sheets Writer — Service Account, limpar aba, escrever dados com mapeamento de colunas.
Inclui retry para erros 429 e ConnectionError (3 tentativas, 5s de intervalo).
"""

import logging
import os
import time
from datetime import datetime

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from app.config import settings

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEETS_MAX_RETRIES = 3
SHEETS_RETRY_INTERVAL = 5  # segundos


def get_sheets_client():
    """
    Retorna um cliente autenticado do Google Sheets via Service Account.

    Carrega o JSON da service account a partir de GOOGLE_CREDENTIALS_PATH.
    Levanta exceção se o arquivo não existir ou for inválido.
    """
    creds_path = os.environ.get("GOOGLE_CREDENTIALS_PATH") or settings.GOOGLE_CREDENTIALS_PATH
    creds_path = os.path.expanduser(creds_path)

    if not os.path.exists(creds_path):
        raise FileNotFoundError(
            f"Arquivo de credenciais da Service Account não encontrado: {creds_path}. "
            "Defina a variável GOOGLE_CREDENTIALS_PATH com o caminho para o JSON da service account."
        )

    try:
        credentials = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    except (ValueError, KeyError) as e:
        raise ValueError(
            f"Arquivo de credenciais inválido: {creds_path}. "
            f"Verifique se o JSON da service account está no formato correto. Erro: {e}"
        )

    return build("sheets", "v4", credentials=credentials)


class SheetsWriter:
    """Escreve dados em uma aba do Google Sheets, preservando o cabeçalho."""

    def __init__(self):
        self.service = get_sheets_client()

    def _extract_sheet_id(self, url: str) -> str:
        """Extrai o ID da planilha da URL."""
        import re
        match = re.search(r"/d/([a-zA-Z0-9-_]+)", url)
        if match:
            return match.group(1)
        raise ValueError(f"URL de planilha inválida: {url}")

    def _retry_sheets_call(self, func, *args, **kwargs):
        """
        Executa uma chamada à API do Google Sheets com retry para erros 429 e ConnectionError.
        Tenta até SHEETS_MAX_RETRIES vezes com intervalo de SHEETS_RETRY_INTERVAL segundos.
        """
        last_exception = None
        for attempt in range(1, SHEETS_MAX_RETRIES + 1):
            try:
                return func(*args, **kwargs)
            except HttpError as e:
                if e.resp.status == 429:
                    last_exception = e
                    logger.warning(
                        f"Google Sheets rate limit (429). Tentativa {attempt}/{SHEETS_MAX_RETRIES}. "
                        f"Aguardando {SHEETS_RETRY_INTERVAL}s..."
                    )
                    time.sleep(SHEETS_RETRY_INTERVAL)
                else:
                    raise
            except ConnectionError as e:
                last_exception = e
                logger.warning(
                    f"Google Sheets ConnectionError. Tentativa {attempt}/{SHEETS_MAX_RETRIES}. "
                    f"Aguardando {SHEETS_RETRY_INTERVAL}s..."
                )
                time.sleep(SHEETS_RETRY_INTERVAL)
        # Todas as tentativas falharam
        raise last_exception

    def write_data(
        self,
        sheets_url: str,
        aba: str,
        data: list,
        mapeamento: dict,
    ) -> dict:
        """
        Limpa dados abaixo do cabeçalho e insere os registros filtrados.

        Args:
            sheets_url: URL da planilha Google Sheets
            aba: Nome da aba (ex: "D+1 - COBRANÇA")
            data: Lista de dicionários com os dados do ERP
            mapeamento: Mapeamento de colunas ERP → Sheets

        Returns:
            dict com status e quantidade de linhas escritas
        """
        spreadsheet_id = self._extract_sheet_id(sheets_url)

        try:
            # 1. Ler o cabeçalho existente (linha 1)
            header_range = f"'{aba}'!1:1"
            header_result = self._retry_sheets_call(
                self.service.spreadsheets().values().get,
                spreadsheetId=spreadsheet_id,
                range=header_range,
            ).execute()
            header_values = header_result.get("values", [[]])[0]

            if not header_values:
                logger.warning(f"Aba '{aba}' não tem cabeçalho. Escrevendo cabeçalho novo.")
                header_values = list(mapeamento.values())

            # 2. Construir mapeamento de coluna (nome → índice)
            col_map = {name: idx for idx, name in enumerate(header_values)}

            # 3. Limpar dados existentes (da linha 2 em diante)
            full_range = f"'{aba}'!A:Z"
            full_result = self._retry_sheets_call(
                self.service.spreadsheets().values().get,
                spreadsheetId=spreadsheet_id,
                range=full_range,
            ).execute()
            existing_rows = len(full_result.get("values", []))

            if existing_rows > 1:
                clear_range = f"'{aba}'!2:{existing_rows}"
                self._retry_sheets_call(
                    self.service.spreadsheets().values().clear,
                    spreadsheetId=spreadsheet_id,
                    range=clear_range,
                    body={},
                ).execute()

            # 4. Preparar dados para inserção
            rows = []
            for record in data:
                row = [""] * len(header_values)
                for erp_col, sheets_col in mapeamento.items():
                    if sheets_col in col_map and erp_col in record:
                        value = record[erp_col]
                        # Formatar datas como DD/MM/AAAA
                        if isinstance(value, datetime):
                            value = value.strftime("%d/%m/%Y")
                        elif isinstance(value, str) and erp_col in (
                            "vencimento_Parcela",
                            "vencimento_parcela",
                            "dt_contrato",
                            "dt_inicio_vigencia",
                        ):
                            try:
                                dt = datetime.fromisoformat(value.replace("Z", ""))
                                value = dt.strftime("%d/%m/%Y")
                            except (ValueError, AttributeError):
                                pass
                        row[col_map[sheets_col]] = str(value) if value != "" else ""
                rows.append(row)

            # 5. Inserir dados a partir da linha 2
            if rows:
                insert_range = f"'{aba}'!A2"
                body = {
                    "values": rows,
                    "majorDimension": "ROWS",
                }
                self._retry_sheets_call(
                    self.service.spreadsheets().values().update,
                    spreadsheetId=spreadsheet_id,
                    range=insert_range,
                    valueInputOption="USER_ENTERED",
                    body=body,
                ).execute()

            logger.info(f"{len(rows)} linhas escritas na aba '{aba}'")
            return {
                "status": "sucesso",
                "linhas_escritas": len(rows),
                "log": f"{len(rows)} registros inseridos na aba '{aba}'",
            }

        except HttpError as e:
            logger.error(f"Erro Google Sheets: {e}")
            return {
                "status": "erro",
                "linhas_escritas": 0,
                "log": f"Erro Google Sheets: {e}",
            }
        except ConnectionError as e:
            logger.error(f"Erro de conexão Google Sheets: {e}")
            return {
                "status": "erro",
                "linhas_escritas": 0,
                "log": f"Erro de conexão Google Sheets após {SHEETS_MAX_RETRIES} tentativas: {e}",
            }