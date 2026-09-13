import operator
import os
from datetime import datetime, timedelta

import clickhouse_connect
import faust
from dotenv import load_dotenv

load_dotenv()

app = faust.App(
    "sptrans-stream-processor",
    broker="kafka://localhost:9094",
    topic_partitions=6,
)

posicoes_topic = app.topic("posicoes_onibus", value_type=None)

veiculos_por_linha = app.Table(
    "veiculos-por-linha",
    default=set,
).tumbling(60, expires=timedelta(minutes=2))

ch_client = clickhouse_connect.get_client(
    host="localhost",
    port=8123,
    username="default",
    password=os.getenv("CLICKHOUSE_PASSWORD"),
)

@app.agent(posicoes_topic)
async def processar_posicoes(eventos):
    async for evento in eventos:
        linha = evento.get("codigo_linha")
        prefixo = evento.get("prefixo_veiculo")

        veiculos_por_linha[linha].apply(operator.or_, {prefixo})
        veiculos_ativos = veiculos_por_linha[linha].current()

        ch_client.insert(
            "veiculos_por_linha",
            [[linha, len(veiculos_ativos), datetime.now()]],
            column_names=["linha", "veiculos_ativos", "timestamp"],
        )

if __name__ == "__main__":
    app.main()