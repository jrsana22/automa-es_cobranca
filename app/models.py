import json
from datetime import datetime
from typing import Optional

from sqlalchemy import Integer, String, Boolean, Text, DateTime, ForeignKey, func as sa_func, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


# Tipos de fluxo disponíveis
FLUXO_PREBOLETO = "preboleto"       # D-7 a D-1 (vencendo em até 7 dias)
FLUXO_VENCENDO_HOJE = "vencendo_hoje"  # D-0 (vence hoje)
FLUXO_COBRANCA_D1 = "cobranca_d1"   # D+1 (1 dia vencido)
FLUXO_COBRANCA_2_30 = "cobranca_2_30"  # 2 a 30 dias vencido (a cada 3 dias)
FLUXO_REATIVACAO = "reativacao"      # 31 a 120 dias vencido

FLUXOS_PADRAO = [
    {"tipo": FLUXO_PREBOLETO,      "nome": "Pré-boleto",       "sheets_aba": "Pré-boleto",        "filtro_dias_min": -7, "filtro_dias_max": -1},
    {"tipo": FLUXO_VENCENDO_HOJE, "nome": "Vencendo Hoje",    "sheets_aba": "Vencendo Hoje",     "filtro_dias_min": 0,  "filtro_dias_max": 0},
    {"tipo": FLUXO_COBRANCA_D1,   "nome": "Cobrança D+1",     "sheets_aba": "D+1",               "filtro_dias_min": 1,  "filtro_dias_max": 1},
    {"tipo": FLUXO_COBRANCA_2_30, "nome": "Cobrança 2-30D",   "sheets_aba": "Cobrança 2-30D",    "filtro_dias_min": 2,  "filtro_dias_max": 30},
    {"tipo": FLUXO_REATIVACAO,    "nome": "Reativação",        "sheets_aba": "Reativação",        "filtro_dias_min": 31, "filtro_dias_max": 120},
]


class Automacao(Base):
    """
    Uma automação = um cliente.
    Contém dados de acesso ao ERP e a planilha Google Sheets.
    Cada automação tem 5 fluxos que filtram os mesmos dados de formas diferentes.
    """
    __tablename__ = "automacoes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    nome: Mapped[str] = mapped_column(String(200), nullable=False)
    ativo: Mapped[bool] = mapped_column(Boolean, default=True)

    # ERP (variável por cliente)
    erp_url: Mapped[str] = mapped_column(String(500), nullable=False)
    erp_login: Mapped[str] = mapped_column(String(200), nullable=False)
    erp_senha: Mapped[str] = mapped_column(Text, nullable=False)  # criptografada
    erp_tipo: Mapped[str] = mapped_column(String(50), default="apvs_brasil")

    # Google Sheets (1 planilha por cliente, cada fluxo escreve em uma aba)
    sheets_url: Mapped[str] = mapped_column(String(500), nullable=False)

    # Filtro de dados (comum a todos os fluxos)
    coluna_vencimento: Mapped[str] = mapped_column(String(100), default="vencimento_Parcela")
    horario_execucao: Mapped[str] = mapped_column(String(5), default="06:00")

    # Dias da semana para execução (0=Seg, 6=Dom, formato APScheduler)
    dias_semana: Mapped[str] = mapped_column(String(20), default="0,1,2,3,4")

    DIAS_SEMANA_NOMES = {0: "Seg", 1: "Ter", 2: "Qua", 3: "Qui", 4: "Sex", 5: "Sáb", 6: "Dom"}

    @property
    def dias_semana_lista(self) -> list[int]:
        if not self.dias_semana:
            return []
        return [int(d.strip()) for d in self.dias_semana.split(",") if d.strip().isdigit()]

    @property
    def dias_semana_display(self) -> str:
        dias = self.dias_semana_lista
        if dias == [0, 1, 2, 3, 4]:
            return "Seg-Sex"
        if dias == [0, 1, 2, 3, 4, 5]:
            return "Seg-Sáb"
        if dias == [0, 1, 2, 3, 4, 5, 6]:
            return "Todos os dias"
        if not dias:
            return "Nenhum"
        return ", ".join(self.DIAS_SEMANA_NOMES.get(d, str(d)) for d in dias)

    # Ciclo de 3 dias para cobrança 2-30D: controla o dia base (1 = dias 1,4,7...)
    dia_cobranca_base: Mapped[int] = mapped_column(Integer, default=1)

    # Mapeamento de colunas ERP → Sheets (JSON, compartilhado entre fluxos)
    mapeamento_json: Mapped[str] = mapped_column(Text, default="{}")

    fluxos: Mapped[list["Fluxo"]] = relationship(back_populates="automacao", cascade="all, delete-orphan")
    execucoes: Mapped[list["Execucao"]] = relationship(back_populates="automacao", cascade="all, delete-orphan")

    @property
    def mapeamento(self) -> dict:
        return json.loads(self.mapeamento_json) if self.mapeamento_json else {}

    @mapeamento.setter
    def mapeamento(self, value: dict):
        self.mapeamento_json = json.dumps(value, ensure_ascii=False)

    # Mapeamento padrão para APVS (coluna ERP → coluna Sheets)
    MAPEAMENTO_PADRAO = {
        "nome": "Nome",
        "placa": "Placa",
        "celular": "Celular",
        "boleto": "Boleto",
        "link": "Link",
        "valor_total": "Valor Total",
        "vencimento_Parcela": "Vencimento",
    }


class Fluxo(Base):
    """
    Cada automação tem 5 fluxos que filtram os mesmos dados do ERP de formas diferentes,
    cada um escrevendo em uma aba separada da planilha.
    """
    __tablename__ = "fluxos"
    __table_args__ = (UniqueConstraint("automacao_id", "tipo", name="uq_fluxo_tipo"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    automacao_id: Mapped[int] = mapped_column(Integer, ForeignKey("automacoes.id"), nullable=False)
    tipo: Mapped[str] = mapped_column(String(20), nullable=False)  # preboleto/vencendo_hoje/cobranca_d1/cobranca_2_30/reativacao
    nome: Mapped[str] = mapped_column(String(100), nullable=False)
    sheets_aba: Mapped[str] = mapped_column(String(200), nullable=False)
    filtro_dias_min: Mapped[int] = mapped_column(Integer, nullable=False)  # negativo=futuro, 0=hoje, positivo=passado
    filtro_dias_max: Mapped[int] = mapped_column(Integer, nullable=False)
    ativo: Mapped[bool] = mapped_column(Boolean, default=True)

    automacao: Mapped["Automacao"] = relationship(back_populates="fluxos")
    execucoes: Mapped[list["Execucao"]] = relationship(back_populates="fluxo", cascade="all, delete-orphan")


class Execucao(Base):
    __tablename__ = "execucoes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    automacao_id: Mapped[int] = mapped_column(Integer, ForeignKey("automacoes.id"), nullable=False)
    fluxo_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("fluxos.id"), nullable=True)
    data: Mapped[datetime] = mapped_column(DateTime, server_default=sa_func.now())
    # pendente / sucesso / erro / parcial / vazio
    status: Mapped[str] = mapped_column(String(20), default="pendente")
    registros_encontrados: Mapped[int] = mapped_column(Integer, default=0)
    registros_filtrados: Mapped[int] = mapped_column(Integer, default=0)
    log: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    automacao: Mapped["Automacao"] = relationship(back_populates="execucoes")
    fluxo: Mapped[Optional["Fluxo"]] = relationship(back_populates="execucoes")