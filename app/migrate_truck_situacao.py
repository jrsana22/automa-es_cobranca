"""
Corrige situacao_id nos fluxos de ERPs do tipo apvs_truck já cadastrados no banco.

Antes: todos os fluxos Truck tinham situacao_id="" (vazio)
Depois:
  - cobranca_d1  → "2" (Inadimplência-Cobrança)
  - cobranca_2_30 → "2" (Inadimplência-Cobrança)
  - reativacao   → "3" (Inadimplência-Reativação)  ← era isso que causava 0 resultados
"""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "automacao.db")

_TRUCK_SITUACOES = {
    "cobranca_d1":   "2",
    "cobranca_2_30": "2",
    "reativacao":    "3",
}


def migrate_truck_situacao():
    if not os.path.exists(DB_PATH):
        print("Banco não encontrado. Nada a migrar.")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='erp_configs'")
    if not cur.fetchone():
        print("Tabela erp_configs não existe. Pulando migrate_truck_situacao.")
        conn.close()
        return

    total = 0
    for tipo_fluxo, situacao_correta in _TRUCK_SITUACOES.items():
        cur.execute("""
            UPDATE fluxos
            SET situacao_id = ?
            WHERE tipo = ?
              AND situacao_id != ?
              AND erp_config_id IN (
                  SELECT id FROM erp_configs WHERE erp_tipo = 'apvs_truck'
              )
        """, (situacao_correta, tipo_fluxo, situacao_correta))
        total += cur.rowcount

    conn.commit()
    conn.close()

    if total:
        print(f"migrate_truck_situacao: {total} fluxo(s) corrigido(s).")
    else:
        print("migrate_truck_situacao: nada a corrigir.")


if __name__ == "__main__":
    migrate_truck_situacao()
