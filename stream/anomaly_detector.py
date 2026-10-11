import logging
import os
import time
from collections import defaultdict
from datetime import datetime

import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("anomaly_detector")

VERIFICAR_INTERVALO_SEGUNDOS = 30
JANELAS = 5
THRESHOLD_PERCENTUAL = -50.0


def buscar_anomalias(ch_client):
    janela_segundos = JANELAS * VERIFICAR_INTERVALO_SEGUNDOS

    resultado = ch_client.query(f"""
        SELECT linha, veiculos_ativos
        FROM veiculos_por_linha
        WHERE timestamp >= now() - INTERVAL {janela_segundos} SECOND
        ORDER BY linha, timestamp DESC
    """)

    por_linha = defaultdict(list)
    for linha, veiculos in resultado.result_rows:
        por_linha[linha].append(veiculos)

    anomalias = []
    for linha, historico in por_linha.items():
        if len(historico) < 2:
            continue
        atual = historico[0]
        media = sum(historico[1:]) / len(historico[1:])
        if media == 0:
            continue
        variacao = (atual - media) / media * 100
        if variacao < THRESHOLD_PERCENTUAL:
            anomalias.append((linha, atual, media, variacao))

    return anomalias


def executar_anomaly_detector():
    ch_client = clickhouse_connect.get_client(
        host="localhost",
        port=8123,
        username="default",
        password=os.getenv("CLICKHOUSE_PASSWORD"),
    )

    logger.info(
        "Anomaly detector iniciado. Threshold: %.0f%% | Janelas: %d | Intervalo: %ds",
        THRESHOLD_PERCENTUAL,
        JANELAS,
        VERIFICAR_INTERVALO_SEGUNDOS,
    )

    while True:
        try:
            anomalias = buscar_anomalias(ch_client)
            if anomalias:
                rows = [
                    [linha, datetime.now(), atual, media, variacao]
                    for linha, atual, media, variacao in anomalias
                ]
                ch_client.insert(
                    "anomalias",
                    rows,
                    column_names=["linha", "timestamp", "veiculos_ativos", "media_movel", "variacao_percentual"],
                )
                logger.warning("Anomalias detectadas: %d linhas afetadas.", len(rows))
            else:
                logger.info("Nenhuma anomalia detectada.")
        except Exception:
            logger.exception("Erro no ciclo de detecção.")

        time.sleep(VERIFICAR_INTERVALO_SEGUNDOS)


if __name__ == "__main__":
    executar_anomaly_detector()
