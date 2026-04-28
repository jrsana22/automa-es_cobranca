"""
Migração do banco de dados para o modelo multi-ERP:
1. Cria tabela erp_configs
2. Para cada automacao existente, cria um ERPConfig com seus campos ERP
3. Atualiza fluxos: adiciona erp_config_id, popula, remove automacao_id
4. Atualiza execucoes: adiciona erp_config_id, popula a partir do fluxo
5. Remove colunas ERP de automacoes (erp_url, erp_login, erp_senha, erp_tipo)
"""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "automacao.db")


def migrate_multi_erp():
    if not os.path.exists(DB_PATH):
        print("Banco não encontrado. Nada a migrar.")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # ---- Idempotência: verifica se a migração já foi feita ----
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='erp_configs'")
    if cur.fetchone():
        print("Tabela 'erp_configs' já existe. Migração multi-ERP já foi feita.")
        conn.close()
        return

    print("Iniciando migração multi-ERP...")

    # ================================================================
    # 1. Criar tabela erp_configs
    # ================================================================
    cur.execute("""
        CREATE TABLE erp_configs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            automacao_id INTEGER NOT NULL,
            erp_tipo VARCHAR(50) NOT NULL,
            erp_url VARCHAR(500) NOT NULL,
            erp_login VARCHAR(200) NOT NULL,
            erp_senha TEXT NOT NULL,
            ativo BOOLEAN DEFAULT 1,
            FOREIGN KEY (automacao_id) REFERENCES automacoes(id),
            UNIQUE (automacao_id, erp_tipo)
        )
    """)
    print("  Tabela 'erp_configs' criada.")

    # ================================================================
    # 2. Para cada automacao, criar um ERPConfig com os campos ERP
    # ================================================================
    cur.execute("SELECT id, erp_url, erp_login, erp_senha, erp_tipo FROM automacoes")
    automacoes = cur.fetchall()

    # Mapeamento automacao_id → erp_config_id (para uso nas etapas 3 e 4)
    auto_to_erp_config = {}

    for auto in automacoes:
        erp_tipo = auto["erp_tipo"] or "apvs_brasil"
        erp_url = auto["erp_url"] or ""
        erp_login = auto["erp_login"] or ""
        erp_senha = auto["erp_senha"] or ""

        cur.execute(
            "INSERT INTO erp_configs (automacao_id, erp_tipo, erp_url, erp_login, erp_senha, ativo) "
            "VALUES (?, ?, ?, ?, ?, 1)",
            (auto["id"], erp_tipo, erp_url, erp_login, erp_senha),
        )
        erp_config_id = cur.lastrowid
        auto_to_erp_config[auto["id"]] = erp_config_id
        print(f"  ERPConfig criado para automacao {auto['id']} (tipo={erp_tipo}) → erp_config_id={erp_config_id}")

    # ================================================================
    # 3. Atualizar fluxos: adicionar erp_config_id, popular, remover automacao_id
    #    SQLite não suporta DROP COLUMN antes da versão 3.35.0,
    #    então recriamos a tabela.
    # ================================================================
    cur.execute("PRAGMA table_info(fluxos)")
    fluxos_cols = [row[1] for row in cur.fetchall()]

    if "erp_config_id" not in fluxos_cols:
        # Adicionar coluna erp_config_id
        cur.execute("ALTER TABLE fluxos ADD COLUMN erp_config_id INTEGER REFERENCES erp_configs(id)")

        # Popular erp_config_id a partir de automacao_id
        for auto_id, erp_config_id in auto_to_erp_config.items():
            cur.execute("UPDATE fluxos SET erp_config_id = ? WHERE automacao_id = ?", (erp_config_id, auto_id))
        print("  Coluna 'erp_config_id' adicionada e populada em fluxos.")

        # Recriar tabela fluxos sem automacao_id
        cur.execute("""
            CREATE TABLE fluxos_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                erp_config_id INTEGER NOT NULL,
                tipo VARCHAR(20) NOT NULL,
                nome VARCHAR(100) NOT NULL,
                sheets_aba VARCHAR(200) NOT NULL,
                filtro_dias_min INTEGER NOT NULL,
                filtro_dias_max INTEGER NOT NULL,
                ativo BOOLEAN DEFAULT 1,
                formulario_id VARCHAR(20) DEFAULT '127000007',
                situacao_id VARCHAR(20) DEFAULT '2',
                FOREIGN KEY (erp_config_id) REFERENCES erp_configs(id),
                UNIQUE (erp_config_id, tipo)
            )
        """)

        # Copiar dados (sem automacao_id)
        copy_cols = ["id", "erp_config_id", "tipo", "nome", "sheets_aba",
                     "filtro_dias_min", "filtro_dias_max", "ativo", "formulario_id", "situacao_id"]
        cur.execute(f"SELECT {','.join(copy_cols)} FROM fluxos")
        rows = cur.fetchall()
        for row in rows:
            values = [row[c] for c in copy_cols]
            placeholders = ",".join(["?"] * len(copy_cols))
            cur.execute(
                f"INSERT INTO fluxos_new ({','.join(copy_cols)}) VALUES ({placeholders})", values
            )

        cur.execute("DROP TABLE fluxos")
        cur.execute("ALTER TABLE fluxos_new RENAME TO fluxos")
        print("  Tabela 'fluxos' recriada sem automacao_id, com erp_config_id.")
    else:
        print("  Coluna 'erp_config_id' já existe em fluxos. Pulando recriação.")

    # ================================================================
    # 4. Atualizar execucoes: adicionar erp_config_id e popular
    # ================================================================
    cur.execute("PRAGMA table_info(execucoes)")
    execucoes_cols = [row[1] for row in cur.fetchall()]

    if "erp_config_id" not in execucoes_cols:
        cur.execute("ALTER TABLE execucoes ADD COLUMN erp_config_id INTEGER REFERENCES erp_configs(id)")

        # Popular erp_config_id a partir do fluxo associado
        # Usa JOIN para buscar o erp_config_id do fluxo
        cur.execute("""
            UPDATE execucoes
            SET erp_config_id = (
                SELECT f.erp_config_id
                FROM fluxos f
                WHERE f.id = execucoes.fluxo_id
            )
            WHERE fluxo_id IS NOT NULL
        """)

        # Para execuções sem fluxo_id, usar automacao_id para encontrar o erp_config
        for auto_id, erp_config_id in auto_to_erp_config.items():
            cur.execute(
                "UPDATE execucoes SET erp_config_id = ? "
                "WHERE automacao_id = ? AND erp_config_id IS NULL",
                (erp_config_id, auto_id),
            )
        print("  Coluna 'erp_config_id' adicionada e populada em execucoes.")
    else:
        print("  Coluna 'erp_config_id' já existe em execucoes. Pulando.")

    # ================================================================
    # 5. Remover colunas ERP de automacoes (erp_url, erp_login, erp_senha, erp_tipo)
    #    SQLite requer recriação da tabela.
    # ================================================================
    cur.execute("PRAGMA table_info(automacoes)")
    auto_cols = [row[1] for row in cur.fetchall()]

    if "erp_url" in auto_cols:
        # Colunas que devem permanecer na nova tabela automacoes
        keep_cols = [
            "id", "nome", "ativo", "sheets_url", "coluna_vencimento",
            "horario_execucao", "dia_cobranca_base", "mapeamento_json", "dias_semana"
        ]
        # Filtrar para apenas colunas que realmente existem no banco atual
        existing_keep = [c for c in keep_cols if c in auto_cols]

        cur.execute("""
            CREATE TABLE automacoes_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome VARCHAR(200) NOT NULL,
                ativo BOOLEAN DEFAULT 1,
                sheets_url VARCHAR(500) NOT NULL,
                coluna_vencimento VARCHAR(100) DEFAULT 'vencimento_Parcela',
                horario_execucao VARCHAR(5) DEFAULT '06:00',
                dia_cobranca_base INTEGER DEFAULT 1,
                mapeamento_json TEXT DEFAULT '{}',
                dias_semana VARCHAR(20) DEFAULT '0,1,2,3,4'
            )
        """)

        cur.execute(f"SELECT {','.join(existing_keep)} FROM automacoes")
        rows = cur.fetchall()
        for row in rows:
            values = [row[c] for c in existing_keep]
            placeholders = ",".join(["?"] * len(existing_keep))
            cur.execute(
                f"INSERT INTO automacoes_new ({','.join(existing_keep)}) VALUES ({placeholders})", values
            )

        cur.execute("DROP TABLE automacoes")
        cur.execute("ALTER TABLE automacoes_new RENAME TO automacoes")
        print("  Colunas ERP removidas da tabela 'automacoes'.")
    else:
        print("  Colunas ERP já removidas de automacoes. Pulando recriação.")

    # ================================================================
    # Commit e encerramento
    # ================================================================
    conn.commit()
    conn.close()
    print("Migração multi-ERP concluída com sucesso!")


if __name__ == "__main__":
    migrate_multi_erp()