"""
Migração do banco de dados:
1. Cria tabela fluxos
2. Adiciona coluna fluxo_id em execucoes
3. Migra dados existentes (sheets_aba + filtro_dias → fluxo)
4. Remove colunas antigas de automacoes
"""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "automacao.db")


def migrate():
    if not os.path.exists(DB_PATH):
        print("Banco não encontrado. Será criado pelo init_db().")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Verifica se já migrou
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='fluxos'")
    if cur.fetchone():
        print("Tabela 'fluxos' já existe. Migração já foi feita.")
        conn.close()
        return

    print("Iniciando migração...")

    # 1. Criar tabela fluxos
    cur.execute("""
        CREATE TABLE fluxos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            automacao_id INTEGER NOT NULL,
            tipo VARCHAR(20) NOT NULL,
            nome VARCHAR(100) NOT NULL,
            sheets_aba VARCHAR(200) NOT NULL,
            filtro_dias_min INTEGER NOT NULL,
            filtro_dias_max INTEGER NOT NULL,
            ativo BOOLEAN DEFAULT 1,
            FOREIGN KEY (automacao_id) REFERENCES automacoes(id)
        )
    """)

    # 2. Adicionar fluxo_id em execucoes
    cur.execute("ALTER TABLE execucoes ADD COLUMN fluxo_id INTEGER REFERENCES fluxos(id)")

    # 3. Migrar dados existentes
    # Para cada automação, criar um fluxo a partir dos dados antigos
    cur.execute("SELECT * FROM automacoes")
    automacoes = cur.fetchall()

    for auto in automacoes:
        filtro_dias = auto["filtro_dias"] if "filtro_dias" in auto.keys() else -1
        sheets_aba = auto["sheets_aba"] if "sheets_aba" in auto.keys() else "Dados"

        # Inferir tipo baseado no filtro_dias
        if filtro_dias == -1:
            tipo = "cobranca_d1"
            nome = "Cobrança D+1"
            filtro_min, filtro_max = 1, 1
        elif filtro_dias == 0:
            tipo = "vencendo_hoje"
            nome = "Vencendo Hoje"
            filtro_min, filtro_max = 0, 0
        else:
            tipo = "cobranca_2_30"
            nome = f"Cobrança {filtro_dias}D"
            filtro_min, filtro_max = filtro_dias, 30

        cur.execute(
            "INSERT INTO fluxos (automacao_id, tipo, nome, sheets_aba, filtro_dias_min, filtro_dias_max, ativo) VALUES (?, ?, ?, ?, ?, ?, 1)",
            (auto["id"], tipo, nome, sheets_aba, filtro_min, filtro_max),
        )
        print(f"  Migrou automação '{auto['nome']}' → fluxo '{nome}'")

    # 4. Remover colunas antigas (SQLite requer recreate da tabela)
    # Criar nova tabela automacoes sem as colunas removidas
    cur.execute("""
        CREATE TABLE automacoes_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome VARCHAR(200) NOT NULL,
            ativo BOOLEAN DEFAULT 1,
            erp_url VARCHAR(500) NOT NULL,
            erp_login VARCHAR(200) NOT NULL,
            erp_senha TEXT NOT NULL,
            erp_tipo VARCHAR(50) DEFAULT 'apvs_brasil',
            sheets_url VARCHAR(500) NOT NULL,
            coluna_vencimento VARCHAR(100) DEFAULT 'vencimento_Parcela',
            horario_execucao VARCHAR(5) DEFAULT '06:00',
            dia_cobranca_base INTEGER DEFAULT 1,
            mapeamento_json TEXT DEFAULT '{}'
        )
    """)

    # Copiar dados
    cols = ["id", "nome", "ativo", "erp_url", "erp_login", "erp_senha", "erp_tipo",
            "sheets_url", "coluna_vencimento", "horario_execucao", "mapeamento_json"]
    cur.execute(f"SELECT {','.join(cols)} FROM automacoes")
    rows = cur.fetchall()
    for row in rows:
        values = [row[c] for c in cols]
        placeholders = ",".join(["?"] * len(cols))
        cur.execute(f"INSERT INTO automacoes_new ({','.join(cols)}) VALUES ({placeholders})", values)

    # Trocar tabelas
    cur.execute("DROP TABLE automacoes")
    cur.execute("ALTER TABLE automacoes_new RENAME TO automacoes")

    conn.commit()
    conn.close()
    print("Migração concluída com sucesso!")


def migrate_add_dias_semana():
    """Adiciona coluna dias_semana na tabela automacoes se não existir."""
    if not os.path.exists(DB_PATH):
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(automacoes)")
    columns = [row[1] for row in cur.fetchall()]

    if "dias_semana" not in columns:
        cur.execute("ALTER TABLE automacoes ADD COLUMN dias_semana VARCHAR(20) DEFAULT '0,1,2,3,4'")
        conn.commit()
        print("Coluna 'dias_semana' adicionada à tabela automacoes.")
    else:
        print("Coluna 'dias_semana' já existe.")

    conn.close()


def migrate_add_fluxo_campos():
    """Adiciona formulario_id e situacao_id na tabela fluxos se não existirem."""
    if not os.path.exists(DB_PATH):
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(fluxos)")
    columns = [row[1] for row in cur.fetchall()]

    # Mapeamento tipo → (formulario_id, situacao_id)
    tipo_map = {
        "preboleto": ("127000008", ""),
        "vencendo_hoje": ("127000008", ""),
        "cobranca_d1": ("127000007", "2"),
        "cobranca_2_30": ("127000007", "2"),
        "reativacao": ("127000007", "2"),
    }

    if "formulario_id" not in columns:
        cur.execute("ALTER TABLE fluxos ADD COLUMN formulario_id VARCHAR(20) DEFAULT '127000007'")
        conn.commit()
        print("Coluna 'formulario_id' adicionada à tabela fluxos.")

        # Preencher valores corretos por tipo
        for tipo, (form_id, sit_id) in tipo_map.items():
            cur.execute("UPDATE fluxos SET formulario_id = ? WHERE tipo = ?", (form_id, tipo))
        conn.commit()
        print("Valores de formulario_id atualizados por tipo.")
    else:
        print("Coluna 'formulario_id' já existe.")

    if "situacao_id" not in columns:
        cur.execute("ALTER TABLE fluxos ADD COLUMN situacao_id VARCHAR(20) DEFAULT '2'")
        conn.commit()
        print("Coluna 'situacao_id' adicionada à tabela fluxos.")

        # Preencher valores corretos por tipo
        for tipo, (form_id, sit_id) in tipo_map.items():
            cur.execute("UPDATE fluxos SET situacao_id = ? WHERE tipo = ?", (sit_id, tipo))
        conn.commit()
        print("Valores de situacao_id atualizados por tipo.")
    else:
        print("Coluna 'situacao_id' já existe.")

    # Atualizar nomes das abas para os novos nomes padrão
    aba_map = {
        "preboleto": "D-7 - PRÉ-BOLETO",
        "vencendo_hoje": "VENCIMENTO NO DIA",
        "cobranca_d1": "D+1 - COBRANÇA",
        "cobranca_2_30": "COBRANÇA 2-30D",
        "reativacao": "REATIVAÇÃO",
    }
    for tipo, aba in aba_map.items():
        cur.execute("UPDATE fluxos SET sheets_aba = ? WHERE tipo = ?", (aba, tipo))
    conn.commit()
    print("Nomes das abas atualizados.")

    conn.close()


if __name__ == "__main__":
    migrate()
    migrate_add_dias_semana()
    migrate_add_fluxo_campos()

    from migrate_multi_erp import migrate_multi_erp
    migrate_multi_erp()