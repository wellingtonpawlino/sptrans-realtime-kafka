import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime

import clickhouse_connect
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("processor")

KAFKA_BOOTSTRAP_SERVERS = "localhost:9094"
KAFKA_TOPIC = "posicoes_onibus"
FLUSH_INTERVAL_SECONDS = 30


def executar_processor():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="stream-processor",
    )

    ch_client = clickhouse_connect.get_client(
        host="localhost",
        port=8123,
        username="default",
        password=os.getenv("CLICKHOUSE_PASSWORD"),
    )

    logger.info("Processor iniciado. Tópico: %s | Flush a cada %ds", KAFKA_TOPIC, FLUSH_INTERVAL_SECONDS)

    veiculos_por_linha: dict[int, set] = defaultdict(set)
    ultimo_flush = time.time()

    for mensagem in consumer:
        evento = mensagem.value
        linha = evento.get("codigo_linha")
        prefixo = evento.get("prefixo_veiculo")

        if linha is not None and prefixo is not None:
            veiculos_por_linha[linha].add(prefixo)

        agora = time.time()
        if agora - ultimo_flush >= FLUSH_INTERVAL_SECONDS and veiculos_por_linha:
            rows = [
                [linha, len(prefixos), datetime.now()]
                for linha, prefixos in veiculos_por_linha.items()
            ]
            try:
                ch_client.insert(
                    "veiculos_por_linha",
                    rows,
                    column_names=["linha", "veiculos_ativos", "timestamp"],
                )
                logger.info("Gravado %d linhas no ClickHouse.", len(rows))
            except Exception:
                logger.exception("Erro ao gravar no ClickHouse.")
            veiculos_por_linha.clear()
            ultimo_flush = agora


if __name__ == "__main__":
    executar_processor()
