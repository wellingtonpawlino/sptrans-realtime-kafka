# SPTrans Realtime Kafka

Pipeline de dados em tempo real das posições dos ônibus de São Paulo, construído sobre a API Olho Vivo (SPTrans) e Apache Kafka.

Projeto derivado de [sptrans-data-pipeline](https://github.com/wellingtonpawlino/sptrans-data-pipeline) (pipeline batch), evoluindo para uma arquitetura de streaming em tempo real.

---

## O problema e como este projeto resolve

São Paulo opera mais de 1.300 linhas de ônibus com milhares de veículos circulando simultaneamente. A API Olho Vivo da SPTrans disponibiliza a posição GPS de cada veículo a cada 45 segundos — um volume de ~6.800 eventos por ciclo.

**A dor:** processar esses dados em batch (executar queries uma vez por hora, por exemplo) significa que qualquer análise operacional chega tarde demais para ser útil. Saber que uma linha ficou sem ônibus por 20 minutos é irrelevante quando você descobre isso no dia seguinte.

**A solução:** uma arquitetura de streaming que ingere, processa e visualiza os dados conforme eles chegam, permitindo:

- Monitorar em tempo real quantos veículos estão ativos por linha
- Detectar anomalias operacionais (linhas com queda brusca de veículos) assim que ocorrem
- Criar uma base histórica de séries temporais para análises futuras

Este projeto demonstra como transformar uma fonte de dados pública em um produto de dados observável e acionável — o mesmo raciocínio que sustenta pipelines de dados em empresas de mobilidade, logística e transporte.

---

## Arquitetura

```
SPTrans API (Olho Vivo)
        │   ~6.800 eventos a cada 45s
        ▼
   [Poller Python]
        │   publica cada veículo como evento individual
        ▼
  Apache Kafka
  tópico: posicoes_onibus (6 partições)
        │   desacopla produção do consumo
        ▼
 [Stream Processor]
        │   agrega veículos únicos por linha em janela de 30s
        ▼
   ClickHouse
   tabela: veiculos_por_linha
        │   armazena séries temporais com alta performance de escrita
        ├──────────────────────────────────┐
        ▼                                  │ consulta últimas 5 janelas
    Grafana                       [Anomaly Detector]
    dashboard em tempo real                │   calcula média móvel por linha
                                           │   detecta quedas > 50%
                                           ▼
                                      ClickHouse
                                      tabela: anomalias
                                           │
                                           ▼
                                       Grafana
                                       alertas sobrepostos ao dashboard
```

---

## Stack e decisões técnicas

### Por que Kafka?

O Kafka atua como buffer entre a coleta e o processamento, resolvendo três problemas de uma vez:

1. **Desacoplamento:** o poller e o processor rodam de forma independente. Se o processor cair, o Kafka retém as mensagens e o processamento retoma de onde parou quando o processo sobe novamente.
2. **Escalabilidade:** com 6 partições, múltiplos consumers podem processar em paralelo caso o volume cresça.
3. **Replay:** é possível reprocessar dados históricos alterando o offset do consumer — útil para corrigir bugs no processamento sem precisar rebuscar a API.

Neste projeto o Kafka roda em **modo KRaft** (sem Zookeeper), o que simplifica a infraestrutura de desenvolvimento e é o padrão a partir do Kafka 3.x.

### Por que ClickHouse?

O ClickHouse é um banco de dados colunar OLAP otimizado para:
- **Alta velocidade de escrita em lote** — ideal para ingestão contínua de séries temporais
- **Queries analíticas rápidas** — `SELECT linha, max(veiculos_ativos)` sobre milhões de linhas em milissegundos
- **Compressão eficiente** — dados de séries temporais com padrões repetitivos comprimem muito bem

A alternativa seria PostgreSQL, mas ele é otimizado para leitura/escrita transacional (OLTP), não para scans analíticos sobre grandes volumes — o padrão de acesso predominante aqui.

### Por que kafka-python ao invés de Faust/Spark Streaming?

**Faust** foi o processador original planejado, mas apresenta incompatibilidade com Python 3.12+ e problemas de serialização com tipos como `set` em suas tabelas de estado. Para um projeto de aprendizado e portfólio, adicionar complexidade sem benefício real seria contra-producente.

**Spark Streaming** seria adequado para escala horizontal e processamento distribuído, mas adiciona overhead operacional significativo para um volume de ~6.800 eventos/ciclo.

A escolha de **kafka-python com aggregação em memória** é a solução mais simples que resolve o problema corretamente: consome eventos, deduplica veículos por linha usando `set`, e faz batch insert no ClickHouse a cada 30 segundos. Simples, eficiente, e fácil de evoluir.

> Engenharia de dados de qualidade é saber quando não adicionar complexidade.

---

## Stack

| Componente | Tecnologia | Versão |
|------------|------------|--------|
| Message broker | Apache Kafka (KRaft) | 4.3 |
| Stream processing | kafka-python | 3.0.11 |
| Data warehouse | ClickHouse | 24.8 |
| Visualização | Grafana | 11.2 |
| API fonte | SPTrans Olho Vivo | v2.1 |

---

## Como rodar

### Pré-requisitos

- Docker e Docker Compose
- Python 3.10+
- Token da API SPTrans ([registre aqui](https://www.sptrans.com.br/desenvolvedores/))

### Setup

1. Instale as dependências:

```bash
pip install -r requirements.txt
```

2. Crie o arquivo `.env` na raiz:

```env
SPTRANS_TOKEN=seu_token_aqui
CLICKHOUSE_PASSWORD=sua_senha
GRAFANA_ADMIN_PASSWORD=sua_senha
```

3. Suba os containers:

```bash
docker compose up -d
```

4. Crie a tabela no ClickHouse:

```bash
python setup_clickhouse.py
```

### Executar

**Terminal 1 — Poller:**

```bash
PYTHONPATH=. python poller/kafka_poller.py
```

**Terminal 2 — Processor:**

```bash
PYTHONPATH=. python stream/processor.py
```

**Terminal 3 — Anomaly Detector:**

```bash
PYTHONPATH=. python stream/anomaly_detector.py
```

A cada 30 segundos o detector consulta as últimas 5 janelas de `veiculos_por_linha`, calcula a média móvel por linha e grava em `anomalias` quando detectar queda superior a 50%.

**Grafana:** http://localhost:3000 — datasource ClickHouse provisionado automaticamente.

---

## Estrutura

```
├── api/
│   ├── autenticacao.py         autenticação via token SPTrans
│   └── buscar_posicao.py       extração e parsing das posições
├── poller/
│   └── kafka_poller.py         loop de coleta e publicação no Kafka
├── stream/
│   ├── processor.py            consumer com agregação e flush para ClickHouse
│   └── anomaly_detector.py     detecta quedas bruscas de veículos por linha
├── grafana/
│   └── provisioning/           datasource provisionado automaticamente
├── setup_clickhouse.py         DDL das tabelas veiculos_por_linha e anomalias
├── docker-compose.yml          Kafka + ClickHouse + Grafana
└── requirements.txt
```

---

## Contexto de aprendizado

Este projeto faz parte de uma jornada de evolução de **analista de dados para engenheiro de dados**. Os conceitos aplicados aqui — streaming, message brokers, bancos colunares, observabilidade — são os mesmos que sustentam pipelines de produção em empresas como iFood, Nubank e Uber no Brasil.

Cada decisão técnica foi tomada com intenção: entender o tradeoff entre simplicidade e escalabilidade, entre ferramentas conhecidas e novas, entre o que funciona agora e o que suporta crescimento futuro.
