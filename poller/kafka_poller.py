import json
import logging
import time

from dotenv import load_dotenv
from kafka import KafkaProducer

from api.autenticacao import autenticar
from api.buscar_posicao import buscar_posicao_veiculos

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("poller")

KAFKA_BOOTSTRAP_SERVERS = "localhost:9094"
KAFKA_TOPIC = "posicoes_onibus"
POLL_INTERVAL_SECONDS = 45
CICLOS_ATE_RENOVAR_SESSAO = 20


def executar_poller():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    logger.info("Poller iniciado. Tópico: %s | Intervalo: %ds", KAFKA_TOPIC, POLL_INTERVAL_SECONDS)

    session = None
    ciclo = 0

    while True:
        ciclo += 1
        try:
            if session is None or ciclo % CICLOS_ATE_RENOVAR_SESSAO == 0:
                session = autenticar()
                if not session:
                    logger.error("Falha na autenticação, tentando de novo no próximo ciclo.")
                    time.sleep(POLL_INTERVAL_SECONDS)
                    continue

            veiculos = buscar_posicao_veiculos(session)
            for v in veiculos:
                producer.send(KAFKA_TOPIC, key=str(v.get("codigo_linha")), value=v)
            producer.flush()

            logger.info("Ciclo %d: publicados %d eventos.", ciclo, len(veiculos))

        except Exception:
            logger.exception("Erro no ciclo %d, sessão será renovada.", ciclo)
            session = None

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    executar_poller()