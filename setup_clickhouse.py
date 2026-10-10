import os

import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()

client = clickhouse_connect.get_client(
    host="localhost",
    port=8123,
    username="default",
    password=os.getenv("CLICKHOUSE_PASSWORD"),
)

client.command("""
    CREATE TABLE IF NOT EXISTS veiculos_por_linha (
        linha       Int32,
        veiculos_ativos Int32,
        timestamp   DateTime
    ) ENGINE = MergeTree()
    ORDER BY (linha, timestamp)
""")

print("Tabela 'veiculos_por_linha' criada (ou já existia).")
