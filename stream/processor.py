import operator
from datetime import timedelta

import faust

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

@app.agent(posicoes_topic)
async def processar_posicoes(eventos):
    async for evento in eventos:
        linha = evento.get("codigo_linha")
        prefixo = evento.get("prefixo_veiculo")

        veiculos_por_linha[linha].apply(operator.or_, {prefixo})

        veiculos_ativos = veiculos_por_linha[linha].current()
        print(f"Linha {linha}: {len(veiculos_ativos)} veiculos ativos na janela atual")

if __name__ == "__main__":
    app.main()