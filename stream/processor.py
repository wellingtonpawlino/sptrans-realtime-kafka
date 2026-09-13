from datetime import timedelta

import faust

app = faust.App(
    "sptrans-stream-processor",
    broker="kafka://localhost:9094",
    topic_partitions=6
)

posicoes_topic = app.topic("posicoes_onibus", value_type=None)

contagem_por_linha = app.Table(
    "contagem-por-linha",
    default=int,
).tumbling(60, expires=timedelta(minutes=2))

@app.agent(posicoes_topic)
async def processar_posicoes(eventos):
    async for evento in eventos:
        linha = evento.get("codigo_linha")
        contagem_por_linha[linha] += 1

        total_janela = contagem_por_linha[linha].current()
        print(f"Linha {linha}: {total_janela} eventos na janela atual")

if __name__ == "__main__":
    app.main()