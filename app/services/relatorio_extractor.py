"""
Extrator de métricas diárias para o Relatório Regional Capitão.
Faz login no ERP erp.apvs.com.br com as credenciais da Caroline e
coleta 6 grupos de indicadores via exportação XLSX.
"""
import logging
import time
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

import pandas as pd

from app.services.erp_apvs import APVSClient

logger = logging.getLogger(__name__)

ERP_URL = "https://erp.apvs.com.br"
ERP_LOGIN = "Caroline.silva"

# CNPJs das 3 regionais do Gil
CNPJ_CAPITAO = "13989780000196"
CNPJ_CAPITAO_2 = "43234290000140"
CNPJ_JARDIM_EUROPA = "50391874000195"

FORM_INADI = "127000007"      # Inadimplência / Reativação
FORM_NOVOS = "127000009"      # Novos Contratos
FORM_RECEB = "127000010"      # Recebimento

SIT_COBRANCA = "2"
SIT_REATIVACAO = "3"


def _mes_anterior(d: date) -> tuple[int, int]:
    if d.month == 1:
        return d.year - 1, 12
    return d.year, d.month - 1


def _normalizar_cnpj(serie: pd.Series) -> pd.Series:
    return serie.astype(str).str.strip().str.replace(r"\.0$", "", regex=True)


@dataclass
class SnapshotDiario:
    data_ref: date

    # Vendas (Novos Contratos mês atual)
    vendas_total: int = 0
    vendas_capitao: int = 0
    vendas_capitao2: int = 0
    vendas_jardim_europa: int = 0

    # Cotações apvs.vc — placeholder V1
    cotacoes_mes: int = 0
    cotacoes_dia_anterior: int = 0

    # Reativação
    reativacao_qtd: int = 0
    reativacao_valor: float = 0.0

    # Primeiro Boleto
    pb_total: int = 0       # novos contratos do mês anterior
    pb_pagos: int = 0       # recebidos no mês atual com data contrato = mês anterior
    pb_valor: float = 0.0

    # Inadimplência Cobrança
    inadi_total_qtd: int = 0
    inadi_total_valor: float = 0.0
    inadi_mes_ant_qtd: int = 0
    inadi_mes_ant_valor: float = 0.0
    inadi_mes_atual_qtd: int = 0
    inadi_mes_atual_valor: float = 0.0

    # Cancelamento — placeholder V1
    cancelamento_qtd: int = 0

    # Recebimento (mês atual)
    receb_total_qtd: int = 0
    receb_total_valor: float = 0.0
    receb_capitao_qtd: int = 0
    receb_capitao_valor: float = 0.0
    receb_capitao2_qtd: int = 0
    receb_capitao2_valor: float = 0.0
    receb_jardim_europa_qtd: int = 0
    receb_jardim_europa_valor: float = 0.0

    log: str = ""
    erros: str = ""


class RelatorioExtractor:

    def __init__(self, erp_senha: str):
        self._senha = erp_senha

    def _client(self) -> APVSClient:
        c = APVSClient(ERP_URL, ERP_LOGIN, self._senha)
        if not c.login():
            raise RuntimeError("Falha no login ERP — verifique a senha de Caroline.silva")
        return c

    def extrair(self, data_ref: Optional[date] = None) -> SnapshotDiario:
        if data_ref is None:
            data_ref = date.today()

        snap = SnapshotDiario(data_ref=data_ref)
        logs: list[str] = []
        erros: list[str] = []

        ano_atual = str(data_ref.year)
        mes_atual = str(data_ref.month)
        ano_ant, mes_ant_n = _mes_anterior(data_ref)

        client = self._client()
        try:
            # 1. VENDAS — Novos Contratos mês atual
            try:
                logs.append("[1/5] Extraindo Vendas (Novos Contratos mês atual)...")
                result = client.exportar_relatorio(
                    FORM_NOVOS,
                    id_ano_consulta=ano_atual,
                    id_mes_consulta=mes_atual,
                )
                df = result.dataframe
                cnpj_col = _encontrar_coluna_cnpj(df)
                if cnpj_col:
                    cnpjs = _normalizar_cnpj(df[cnpj_col])
                    snap.vendas_capitao = int((cnpjs == CNPJ_CAPITAO).sum())
                    snap.vendas_capitao2 = int((cnpjs == CNPJ_CAPITAO_2).sum())
                    snap.vendas_jardim_europa = int((cnpjs == CNPJ_JARDIM_EUROPA).sum())
                snap.vendas_total = snap.vendas_capitao + snap.vendas_capitao2 + snap.vendas_jardim_europa
                logs.append(
                    f"Vendas: total={snap.vendas_total} "
                    f"(Cap={snap.vendas_capitao}, Cap2={snap.vendas_capitao2}, JE={snap.vendas_jardim_europa})"
                )
            except Exception as e:
                erros.append(f"Vendas: {e}")
                logger.error(f"Erro Vendas: {e}")
            time.sleep(3)

            # 2. REATIVAÇÃO — form 127000007, situação 3
            try:
                logs.append("[2/5] Extraindo Reativação...")
                result = client.exportar_relatorio(FORM_INADI, id_situacao_inadimplente=SIT_REATIVACAO)
                df = result.dataframe
                snap.reativacao_qtd = len(df)
                col_val = _encontrar_coluna(df, ["valor_total"])
                snap.reativacao_valor = float(df[col_val].sum()) if col_val and len(df) > 0 else 0.0
                logs.append(f"Reativação: {snap.reativacao_qtd} contratos, R${snap.reativacao_valor:,.2f}")
            except Exception as e:
                erros.append(f"Reativação: {e}")
                logger.error(f"Erro Reativação: {e}")
            time.sleep(3)

            # 3. INADIMPLÊNCIA COBRANÇA — form 127000007, situação 2
            try:
                logs.append("[3/5] Extraindo Inadimplência...")
                result = client.exportar_relatorio(FORM_INADI, id_situacao_inadimplente=SIT_COBRANCA)
                df = result.dataframe
                snap.inadi_total_qtd = len(df)
                col_val = _encontrar_coluna(df, ["valor_total"])
                col_venc = _encontrar_coluna(df, ["vencimento_Orignal", "vencimento_original", "vencimento_Parcela"])
                snap.inadi_total_valor = float(df[col_val].sum()) if col_val and len(df) > 0 else 0.0
                if col_venc and len(df) > 0:
                    df["_venc_dt"] = pd.to_datetime(df[col_venc], dayfirst=True, errors="coerce")
                    df_ant = df[df["_venc_dt"].dt.month == mes_ant_n]
                    df_atual = df[df["_venc_dt"].dt.month == data_ref.month]
                    snap.inadi_mes_ant_qtd = len(df_ant)
                    snap.inadi_mes_ant_valor = float(df_ant[col_val].sum()) if col_val else 0.0
                    snap.inadi_mes_atual_qtd = len(df_atual)
                    snap.inadi_mes_atual_valor = float(df_atual[col_val].sum()) if col_val else 0.0
                logs.append(
                    f"Inadimplência: total={snap.inadi_total_qtd} R${snap.inadi_total_valor:,.2f} | "
                    f"mês ant={snap.inadi_mes_ant_qtd} | mês atual={snap.inadi_mes_atual_qtd}"
                )
            except Exception as e:
                erros.append(f"Inadimplência: {e}")
                logger.error(f"Erro Inadimplência: {e}")
            time.sleep(3)

            # 4. PRIMEIRO BOLETO TOTAL — Novos Contratos mês anterior
            try:
                logs.append("[4/5] Extraindo Novos Contratos mês anterior (Primeiro Boleto total)...")
                result = client.exportar_relatorio(
                    FORM_NOVOS,
                    id_ano_consulta=str(ano_ant),
                    id_mes_consulta=str(mes_ant_n),
                )
                snap.pb_total = result.total_registros
                logs.append(f"Primeiro Boleto total: {snap.pb_total}")
            except Exception as e:
                erros.append(f"Primeiro Boleto total: {e}")
                logger.error(f"Erro Primeiro Boleto total: {e}")
            time.sleep(3)

            # 5. RECEBIMENTO — mês atual (inclui cálculo Primeiro Boleto pagos)
            try:
                logs.append("[5/5] Extraindo Recebimento mês atual...")
                result = client.exportar_relatorio(
                    FORM_RECEB,
                    id_ano_consulta=ano_atual,
                    id_mes_consulta=mes_atual,
                    cd_referencia="1",
                )
                df = result.dataframe
                col_val = _encontrar_coluna(df, ["Valor Parcela", "valor_parcela", "Valor_Parcela"])
                col_cnpj = _encontrar_coluna_cnpj(df)
                col_dt_contr = _encontrar_coluna(df, ["Data Contrato", "data_contrato", "Data_Contrato"])

                snap.receb_total_qtd = len(df)
                snap.receb_total_valor = float(df[col_val].sum()) if col_val and len(df) > 0 else 0.0

                if col_cnpj and len(df) > 0:
                    cnpjs = _normalizar_cnpj(df[col_cnpj])
                    for cnpj, attr_q, attr_v in [
                        (CNPJ_CAPITAO, "receb_capitao_qtd", "receb_capitao_valor"),
                        (CNPJ_CAPITAO_2, "receb_capitao2_qtd", "receb_capitao2_valor"),
                        (CNPJ_JARDIM_EUROPA, "receb_jardim_europa_qtd", "receb_jardim_europa_valor"),
                    ]:
                        mask = cnpjs == cnpj
                        setattr(snap, attr_q, int(mask.sum()))
                        setattr(snap, attr_v, float(df.loc[mask, col_val].sum()) if col_val else 0.0)

                # Primeiro Boleto pagos: data contrato pertence ao mês anterior
                if col_dt_contr and col_val and len(df) > 0:
                    df["_dt_contr"] = pd.to_datetime(df[col_dt_contr], dayfirst=True, errors="coerce")
                    mask_pb = (df["_dt_contr"].dt.year == ano_ant) & (df["_dt_contr"].dt.month == mes_ant_n)
                    df_pb = df[mask_pb]
                    snap.pb_pagos = len(df_pb)
                    snap.pb_valor = float(df_pb[col_val].sum()) if len(df_pb) > 0 else 0.0

                logs.append(
                    f"Recebimento: {snap.receb_total_qtd} placas R${snap.receb_total_valor:,.2f} | "
                    f"1º Boleto pagos={snap.pb_pagos}/{snap.pb_total}"
                )
            except Exception as e:
                erros.append(f"Recebimento: {e}")
                logger.error(f"Erro Recebimento: {e}")

        finally:
            client.close()

        snap.log = "\n".join(logs)
        snap.erros = "\n".join(erros)
        return snap


def _encontrar_coluna_cnpj(df: pd.DataFrame) -> Optional[str]:
    for candidato in ["CPF/CNPJ Regional", "CNPJ Regional", "cnpj_regional", "cpf_cnpj_regional"]:
        if candidato in df.columns:
            return candidato
    for col in df.columns:
        if "cnpj" in col.lower() or "cpf" in col.lower():
            return col
    return None


def _encontrar_coluna(df: pd.DataFrame, candidatos: list[str]) -> Optional[str]:
    for c in candidatos:
        if c in df.columns:
            return c
    return None
