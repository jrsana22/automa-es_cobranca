import json
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

from sqlalchemy import Integer, String, Boolean, Text, DateTime, ForeignKey, UniqueConstraint

_BRASILIA = ZoneInfo("America/Sao_Paulo")


def _agora_brasilia():
    return datetime.now(_BRASILIA)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


# Tipos de fluxo disponíveis
FLUXO_PREBOLETO = "preboleto"       # D-7 a D-1 (vencendo em até 7 dias)
FLUXO_VENCENDO_HOJE = "vencendo_hoje"  # D-0 (vence hoje)
FLUXO_COBRANCA_D1 = "cobranca_d1"   # D+1 (1 dia vencido)
FLUXO_COBRANCA_2_30 = "cobranca_2_30"  # 2 a 30 dias vencido (a cada 3 dias)
FLUXO_REATIVACAO = "reativacao"      # 31 a 120 dias vencido

FLUXOS_PADRAO = [
    {"tipo": FLUXO_PREBOLETO,      "nome": "Pré-boleto",          "sheets_aba": "D-7 - PRÉ-BOLETO",                              "formulario_id": "127000008", "situacao_id": "", "filtro_dias_min": 1,    "filtro_dias_max": 7},
    {"tipo": FLUXO_VENCENDO_HOJE,  "nome": "Vencimento no Dia",   "sheets_aba": "VENCIMENTO NO DIA",                             "formulario_id": "127000007", "situacao_id": "", "filtro_dias_min": 0,    "filtro_dias_max": 0},
    {"tipo": FLUXO_COBRANCA_D1,    "nome": "Cobrança D+1",        "sheets_aba": "D+1 - COBRANÇA",                                "formulario_id": "127000007", "situacao_id": "", "filtro_dias_min": -1,  "filtro_dias_max": -1},
    {"tipo": FLUXO_COBRANCA_2_30,  "nome": "Cobrança 2-30D",      "sheets_aba": "COBRANÇA - 2 DIAS VENCIDO A CADA 3 DIAS",      "formulario_id": "127000007", "situacao_id": "", "filtro_dias_min": -30, "filtro_dias_max": -2},
    {"tipo": FLUXO_REATIVACAO,     "nome": "Reativação",           "sheets_aba": "REATIVAÇÃO",                                    "formulario_id": "127000007", "situacao_id": "", "filtro_dias_min": -120,"filtro_dias_max": -31},
]

FLUXOS_PADRAO_TRUCK = [
    {"tipo": FLUXO_PREBOLETO,      "nome": "Pré-boleto",          "sheets_aba": "TRUCK - D-7 - PRÉ-BOLETO",              "formulario_id": "127000008", "situacao_id": "", "filtro_dias_min": 1,    "filtro_dias_max": 7},
    {"tipo": FLUXO_VENCENDO_HOJE,  "nome": "Vencimento no Dia",   "sheets_aba": "TRUCK VENCIMENTO NO DIA",               "formulario_id": "127000007", "situacao_id": "", "filtro_dias_min": 0,    "filtro_dias_max": 0},
    {"tipo": FLUXO_COBRANCA_D1,    "nome": "Cobrança D+1",        "sheets_aba": "TRUCK - INADIMPLÊNCIA D+1",             "formulario_id": "127000007", "situacao_id": "", "filtro_dias_min": -1,  "filtro_dias_max": -1},
    {"tipo": FLUXO_COBRANCA_2_30,  "nome": "Cobrança 2-30D",      "sheets_aba": "TRUCK - COBRANÇA - 2 DIAS VENCIDO",     "formulario_id": "127000007", "situacao_id": "", "filtro_dias_min": -30, "filtro_dias_max": -2},
    {"tipo": FLUXO_REATIVACAO,     "nome": "Reativação",           "sheets_aba": "TRUCK - REATIVAÇÃO",                    "formulario_id": "127000007", "situacao_id": "", "filtro_dias_min": -120,"filtro_dias_max": -31},
]


def get_fluxos_padrao(erp_tipo: str = "apvs_brasil") -> list[dict]:
    """Retorna FLUXOS_PADRAO ou FLUXOS_PADRAO_TRUCK baseado no tipo de ERP."""
    if erp_tipo == "apvs_truck":
        return FLUXOS_PADRAO_TRUCK
    return FLUXOS_PADRAO


ERP_TIPOS = {
    "apvs_brasil": "APVS Brasil",
    "apvs_truck": "APVS Truck",
}


class Automacao(Base):
    """
    Uma automação = um cliente.
    Contém uma planilha Google Sheets e 1 ou mais ERPs (Brasil, Truck).
    Cada ERP tem 5 fluxos que filtram os mesmos dados de formas diferentes.
    """
    __tablename__ = "automacoes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    nome: Mapped[str] = mapped_column(String(200), nullable=False)
    ativo: Mapped[bool] = mapped_column(Boolean, default=True)

    # Google Sheets (1 planilha por cliente, cada fluxo escreve em uma aba)
    sheets_url: Mapped[str] = mapped_column(String(500), nullable=False)

    # Filtro de dados (comum a todos os fluxos)
    coluna_vencimento: Mapped[str] = mapped_column(String(100), default="vencimento_Parcela")
    horario_execucao: Mapped[str] = mapped_column(String(5), default="06:00")

    # Dias da semana para execução (0=Seg, 6=Dom, formato APScheduler)
    dias_semana: Mapped[str] = mapped_column(String(20), default="0,1,2,3,4")

    # Ciclo de 3 dias para cobrança 2-30D: controla o dia base (1 = dias 1,4,7...)
    dia_cobranca_base: Mapped[int] = mapped_column(Integer, default=1)

    # Mapeamento de colunas ERP → Sheets (JSON, compartilhado entre fluxos)
    mapeamento_json: Mapped[str] = mapped_column(Text, default="{}")

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

    @property
    def mapeamento(self) -> dict:
        return json.loads(self.mapeamento_json) if self.mapeamento_json else {}

    @mapeamento.setter
    def mapeamento(self, value: dict):
        self.mapeamento_json = json.dumps(value, ensure_ascii=False)

    # Mapeamento padrão para APVS (coluna ERP → nome exato da coluna no Sheets)
    MAPEAMENTO_PADRAO = {
        "nome": "Nome",
        "placa": "placa",
        "celular": "telefone_formatado",
        "boleto": "codigo_de_barras (COLAR AQUI)",
        "link": "codigojunto",
        "valor_total": "Valor da mensalidade",
        "vencimento_Parcela": "Vencimento",
        "vencimento": "Vencimento",
    }

    # Relationships
    erp_configs: Mapped[list["ERPConfig"]] = relationship(back_populates="automacao", cascade="all, delete-orphan")
    execucoes: Mapped[list["Execucao"]] = relationship(back_populates="automacao", cascade="all, delete-orphan")
    runs: Mapped[list["AutomacaoRun"]] = relationship(back_populates="automacao", cascade="all, delete-orphan")

    @property
    def all_fluxos(self) -> list["Fluxo"]:
        """Retorna todos os fluxos de todos os ERPs configurados."""
        fluxos = []
        for erp in self.erp_configs:
            fluxos.extend(erp.fluxos)
        return fluxos


class ERPConfig(Base):
    """
    Configuração de um ERP por cliente.
    Um cliente pode ter APVS Brasil, APVS Truck ou ambos.
    Cada ERPConfig tem 5 fluxos (Pré-boleto, Vencendo Hoje, D+1, 2-30D, Reativação).
    """
    __tablename__ = "erp_configs"
    __table_args__ = (UniqueConstraint("automacao_id", "erp_tipo", name="uq_erp_config_tipo"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    automacao_id: Mapped[int] = mapped_column(Integer, ForeignKey("automacoes.id"), nullable=False)
    erp_tipo: Mapped[str] = mapped_column(String(50), nullable=False)  # apvs_brasil / apvs_truck

    # Credenciais do ERP (variável por cliente)
    erp_url: Mapped[str] = mapped_column(String(500), nullable=False)
    erp_login: Mapped[str] = mapped_column(String(200), nullable=False)
    erp_senha: Mapped[str] = mapped_column(Text, nullable=False)  # criptografada

    ativo: Mapped[bool] = mapped_column(Boolean, default=True)

    # Relationships
    automacao: Mapped["Automacao"] = relationship(back_populates="erp_configs")
    fluxos: Mapped[list["Fluxo"]] = relationship(back_populates="erp_config", cascade="all, delete-orphan")
    execucoes: Mapped[list["Execucao"]] = relationship(back_populates="erp_config", cascade="all, delete-orphan")


class Fluxo(Base):
    """
    Cada ERPConfig tem 5 fluxos que filtram os mesmos dados do ERP de formas diferentes,
    cada um escrevendo em uma aba separada da planilha.
    """
    __tablename__ = "fluxos"
    __table_args__ = (UniqueConstraint("erp_config_id", "tipo", name="uq_fluxo_tipo"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    erp_config_id: Mapped[int] = mapped_column(Integer, ForeignKey("erp_configs.id"), nullable=False)
    tipo: Mapped[str] = mapped_column(String(20), nullable=False)  # preboleto/vencendo_hoje/cobranca_d1/cobranca_2_30/reativacao
    nome: Mapped[str] = mapped_column(String(100), nullable=False)
    sheets_aba: Mapped[str] = mapped_column(String(200), nullable=False)
    filtro_dias_min: Mapped[int] = mapped_column(Integer, nullable=False)  # negativo=passado, 0=hoje, positivo=futuro
    filtro_dias_max: Mapped[int] = mapped_column(Integer, nullable=False)  # preboleto: placeholder; processor calcula dias úteis
    ativo: Mapped[bool] = mapped_column(Boolean, default=True)
    formulario_id: Mapped[str] = mapped_column(String(20), default="127000007")
    situacao_id: Mapped[str] = mapped_column(String(20), default="2")

    erp_config: Mapped["ERPConfig"] = relationship(back_populates="fluxos")
    execucoes: Mapped[list["Execucao"]] = relationship(back_populates="fluxo", cascade="all, delete-orphan")


class Execucao(Base):
    __tablename__ = "execucoes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    automacao_id: Mapped[int] = mapped_column(Integer, ForeignKey("automacoes.id"), nullable=False)
    erp_config_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("erp_configs.id"), nullable=True)
    fluxo_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("fluxos.id"), nullable=True)
    data: Mapped[datetime] = mapped_column(DateTime, default=_agora_brasilia)
    # pendente / sucesso / erro / parcial / vazio
    status: Mapped[str] = mapped_column(String(20), default="pendente")
    registros_encontrados: Mapped[int] = mapped_column(Integer, default=0)
    registros_filtrados: Mapped[int] = mapped_column(Integer, default=0)
    log: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    automacao: Mapped["Automacao"] = relationship(back_populates="execucoes")
    erp_config: Mapped[Optional["ERPConfig"]] = relationship(back_populates="execucoes")
    fluxo: Mapped[Optional["Fluxo"]] = relationship(back_populates="execucoes")


class AutomacaoRun(Base):
    """Log completo de cada execução da automação (1 registro por run, não por fluxo)."""
    __tablename__ = "automacao_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    automacao_id: Mapped[int] = mapped_column(Integer, ForeignKey("automacoes.id"), nullable=False)
    data: Mapped[datetime] = mapped_column(DateTime, default=_agora_brasilia)
    # sucesso / parcial / erro
    status: Mapped[str] = mapped_column(String(20), default="sucesso")
    agendado: Mapped[bool] = mapped_column(Boolean, default=False)
    registros_encontrados: Mapped[int] = mapped_column(Integer, default=0)
    registros_filtrados: Mapped[int] = mapped_column(Integer, default=0)
    log_completo: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    duracao_segundos: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    automacao: Mapped["Automacao"] = relationship(back_populates="runs")