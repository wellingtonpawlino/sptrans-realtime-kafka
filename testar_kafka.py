from dotenv import load_dotenv
load_dotenv()

import json
from kafka import KafkaProducer
from api.autenticacao import autenticar
from api.buscar_posicao import buscar_posicao_veiculos

# Conecta no Kafka pela porta externa (9094), já que estamos rodando
# fora do Docker, direto do Windows
producer = KafkaProducer(
    bootstrap_servers="localhost:9094",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

session = autenticar()
veiculos = buscar_posicao_veiculos(session)

print(f"Publicando {len(veiculos)} eventos no tópico 'posicoes_onibus'...")
for v in veiculos:
    producer.send("posicoes_onibus", value=v)

producer.flush()
print("✅ Publicação concluída!")