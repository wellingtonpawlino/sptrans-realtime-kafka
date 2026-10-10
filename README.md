# SPTrans Realtime Kafka

Pipeline de dados em tempo real das posições dos ônibus de São Paulo,
usando a API Olho Vivo (SPTrans) e Apache Kafka.

Projeto derivado de [sptrans-data-pipeline](https://github.com/wellingtonpawlino/sptrans-data-pipeline)
(pipeline batch), evoluindo para uma arquitetura de streaming em tempo real.

## Status

Fase 2 concluída — stream processor gravando no ClickHouse, dashboard no Grafana.

## Arquitetura

```
SPTrans API (Olho Vivo)
        │
        ▼
   [Poller Python]          coleta posições a cada 45s
        │
        ▼
  Apache Kafka              tópico: posicoes_onibus (6 partições)
        │
        ▼
 [Stream Processor]         agrega veículos únicos por linha (janela 30s)
        │
        ▼
   ClickHouse               tabela: veiculos_por_linha
        │
        ▼
    Grafana                 dashboard em tempo real
```

## Stack

| Componente | Tecnologia |
|------------|------------|
| Message broker | Apache Kafka 4.3 (modo KRaft) |
| Stream processing | kafka-python 3.0 |
| Data warehouse | ClickHouse 24.8 |
| Visualização | Grafana 11.2 |
| API fonte | SPTrans Olho Vivo v2.1 |

## Como rodar

### Pré-requisitos

- Docker e Docker Compose
- Python 3.10+
- Token da API SPTrans ([registre aqui](https://www.sptrans.com.br/desenvolvedores/))

### Setup

1. Clone o repositório e instale as dependências:

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

**Terminal 1 — Poller** (coleta da API e publica no Kafka):

```bash
PYTHONPATH=. python poller/kafka_poller.py
```

**Terminal 2 — Processor** (consome Kafka e grava no ClickHouse):

```bash
PYTHONPATH=. python stream/processor.py
```

**Grafana:** acesse http://localhost:3000 (admin / senha do .env)

O datasource ClickHouse é provisionado automaticamente.

## Estrutura

```
├── api/                    integração com API SPTrans
│   ├── autenticacao.py     autenticação via token
│   └── buscar_posicao.py   busca posição dos veículos
├── poller/
│   └── kafka_poller.py     loop de coleta e publicação no Kafka
├── stream/
│   └── processor.py        consumer Kafka com agregação e flush para ClickHouse
├── grafana/
│   └── provisioning/       datasource ClickHouse provisionado automaticamente
├── setup_clickhouse.py     cria tabela veiculos_por_linha
├── docker-compose.yml      Kafka + ClickHouse + Grafana
└── requirements.txt
```
