# Pipeline SELIC — Apache Airflow

Pipeline de dados on-premise orquestrado com Apache Airflow para ingestão, transformação e agregação da Taxa SELIC diária disponibilizada pelo Banco Central do Brasil (BCB).

---

## 1. Descrição do Projeto

Este pipeline consome dados reais da API pública do Banco Central do Brasil — especificamente a **Taxa SELIC diária** (série BCB SGS 11) — e os processa em três camadas conforme a arquitetura medallion: **Bronze → Silver → Gold**.

O ambiente é 100% on-premise, orquestrado com **Apache Airflow 3.1.0** e containerizado via **Docker Compose**. A DAG `Selic` é acionada manualmente (`schedule=None`), com parâmetros de data configuráveis pelo usuário diretamente na interface do Airflow.

**API consumida:**
```
https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json&dataInicial={dataInicial}&dataFinal={dataFinal}
```

---

## 2. Arquitetura

O pipeline segue a **arquitetura medallion** com três camadas de refinamento progressivo dos dados:

| Camada | Responsabilidade |
|--------|-----------------|
| **Bronze** | Ingestão bruta da API, persistência em Parquet com timestamp de carga |
| **Silver** | Concatenação dos arquivos Bronze, remoção de duplicatas, conversão de tipos e validações de qualidade |
| **Gold** | Agregações analíticas (média mensal, variação mensal, taxa acumulada anual) e exportação para XLSX |



### Fluxo de Tasks na DAG

```
ingest  ──►  transform  ──►  aggregate
```

---

## 3. Estrutura de Diretórios

```
be-case/
├── .env                        # Variáveis de ambiente (AIRFLOW_UID, imagem)
├── Dockerfile                  # Imagem customizada com dependências extras
├── docker-compose.yaml         # Orquestração dos serviços Airflow + PostgreSQL
├── requirements.txt            # Dependências Python
├── run.sh                      # Atalho: docker compose build && up -d
│
├── config/                     # Configurações do Airflow (airflow.cfg)
├── dags/
│   └── selic.py                # Definição da DAG e encadeamento de tasks
│
├── include/
│   ├── bronze/
│   │   └── selic_task1.py      # Ingestão da API e salvamento em Parquet
│   ├── silver/
│   │   └── selic_task2.py      # Transformações e checagens de qualidade
│   └── gold/
│       └── selic_task3.py      # Agregações analíticas e exportação XLSX
│
├── data/                       # Volume de dados (gerado em runtime)
│   ├── bronze/selic/           # Parquets brutos (um arquivo por execução)
│   ├── silver/selic/           # selic_clean.parquet
│   └── gold/selic/             # selic_metrics.parquet + selic_metrics.xlsx
│
├── logs/                       # Logs do Airflow
└── plugins/                    # Plugins customizados do Airflow
```

---

## 4. Decisões Técnicas

### Sem uso de XCom

A comunicação entre tasks é feita exclusivamente via **leitura e escrita em disco** (arquivos Parquet), sem o uso de XCom do Airflow.

- A task Bronze salva um arquivo Parquet por execução, nomeado com o timestamp (`selic_YYYYMMDD_HHMMSS.parquet`).
- A task Silver lê **todos** os arquivos `.parquet` presentes no diretório Bronze, os concatena e remove duplicatas antes de persistir o resultado limpo.
- A task Gold lê o arquivo Silver consolidado.

Essa abordagem foi escolhida porque o XCom do Airflow é adequado apenas para metadados ou pequenas mensagens (IDs, caminhos, contagens). Trafegar conjuntos de dados via XCom imporia limites de tamanho e acoplaria as tasks ao estado interno do banco de metadados do Airflow, dificultando o reprocessamento e o rastreio de dados históricos. O uso de arquivos em disco mantém as tasks desacopladas, permite reprocessamento independente e facilita auditoria.

### Datas de Consulta Dinâmicas

Os parâmetros `date_start` e `date_end` são configuráveis pelo usuário via **DAG Params**, definidos diretamente na interface do Airflow no momento do acionamento manual da DAG ("Trigger DAG w/ config").

Os valores padrão definidos na DAG são:
- `date_start`: `01/01/2020`
- `date_end`: `31/12/2024`

O Airflow injeta os parâmetros na task Bronze via Jinja templating:
```python
op_kwargs={
    "date_start": "{{ params.date_start }}",
    "date_end": "{{ params.date_end }}",
}
```

### Escolha do Pandas

O volume de dados envolvido (cotações diárias da SELIC, tipicamente alguns milhares de registros por período) é reduzido e o ambiente é on-premise com processamento single-node. O Pandas atende plenamente a esse perfil, oferecendo API expressiva para transformações tabulares com baixa sobrecarga operacional.

> Em cenários com maior volume de dados ou infraestrutura distribuída, a transformação seria feita diretamente no banco de dados (ex: SQL sobre PostgreSQL/DW) ou com frameworks como **PySpark** ou **DuckDB**, evitando a carga do dataset inteiro em memória e aproveitando processamento paralelo.

### Exportação para XLSX

Além do Parquet (formato otimizado para pipelines), a camada Gold também exporta os resultados em `.xlsx` via `openpyxl`. Essa saída foi incluída exclusivamente para facilitar a visualização do avaliador, permitindo inspecionar os dados finais diretamente no Excel sem necessidade de ferramentas adicionais. Em um ambiente produtivo, essa etapa seria removida, mantendo apenas o Parquet como formato de saída padrão da camada Gold.

### Tratamento de Erros e Retentativas

Configurado na DAG via `default_args`:

```python
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
}
```

- **3 tentativas** automáticas em caso de falha, com intervalo de **1 minuto** entre elas.
- Notificações por e-mail desabilitadas (`email_on_failure=False`).

A task Bronze trata explicitamente duas classes de erro da API:

| Exceção | Comportamento |
|---------|--------------|
| `requests.exceptions.Timeout` | Lança `RuntimeError` com mensagem descritiva; o Airflow reencaminha para retry |
| `requests.exceptions.HTTPError` | Lança `RuntimeError` com o código HTTP retornado |
| Resposta vazia da API | Lança `ValueError` interrompendo a execução |

A task Gold verifica se o arquivo Silver existe antes de processar, lançando `FileNotFoundError` caso contrário.

---

## 5. Transformações Aplicadas

### Bronze

- Requisição HTTP GET à API do BCB com `timeout=60s` e parâmetros de data dinâmicos.
- Adição da coluna `datetime_insert` com o timestamp exato da ingestão (`YYYY-MM-DD HH:MM:SS`), registrando a origem temporal de cada carga.
- Persistência em Parquet usando **PyArrow** (`pa.Table.from_pylist` + `pq.write_table`), com criação automática de diretórios.
- Cada execução gera um arquivo independente com sufixo de timestamp, permitindo histórico de ingestões.

### Silver

- **Concatenação** de todos os arquivos `.parquet` do diretório Bronze em um único DataFrame.
- **Validação estrutural** por arquivo: garante presença das colunas `data`, `valor` e `datetime_insert` antes de incluir o arquivo na concatenação (lança `ValueError` se falhar).
- **Conversão de tipos**:
  - `valor`: `str` → `float`, arredondado para 2 casas decimais.
  - `data`: `str` no formato `%d/%m/%Y` → `datetime`.
  - `datetime_insert`: `str` → `datetime`.
- **Remoção de nulos**: registros com `valor` nulo são descartados (`dropna`).
- **Deduplicação**: ordena por `datetime_insert` decrescente e mantém apenas o registro mais recente para cada data (`drop_duplicates(subset=['data'], keep='first')`), garantindo que reprocessamentos não criem duplicatas.
- **Ordenação final**: reordena por `data` crescente e reinicia o índice.
- Persistência em `selic_clean.parquet`.

### Gold

- Extração de colunas auxiliares `ano` e `mes` a partir de `data`.
- **Média mensal** (`media_mensal`): média da taxa SELIC agrupada por `[ano, mes]`, arredondada a 4 casas decimais.
- **Variação mensal** (`variacao_mensal`): variação percentual mês a mês via `pct_change()` multiplicado por 100, com 0 no primeiro período, arredondada a 4 casas decimais.
- **Taxa acumulada anual** (`taxa_acumulada_anual`): cálculo de juros compostos sobre as taxas diárias do ano — `((1 + taxa/100).prod() - 1) * 100` — agrupado por `ano`, arredondado a 4 casas decimais.
- Merge dos resultados mensais e anuais em um único DataFrame.
- Persistência em `selic_metrics.parquet` e exportação simultânea para `selic_metrics.xlsx`.

---

## 6. Checagens de Qualidade de Dados

> **Comportamento padrão:** caso alguma checagem falhe, um **aviso de logging** é emitido (`logging.warning(...)`) e o fluxo **continua normalmente** — sem interromper a DAG. Apenas a validação estrutural (colunas obrigatórias) é bloqueante, lançando exceção.

As checagens são executadas na camada Silver após as transformações:

| Checagem | Função | Threshold | Comportamento em falha |
|----------|--------|-----------|----------------------|
| **Estrutura** | `structure_validation()` | Colunas: `data`, `valor`, `datetime_insert` | **Bloqueante** — lança `ValueError`; arquivo não é incluído na concatenação |
| **Volume mínimo** | `volume_validation()` | `MIN_RECORDS = 100` registros | `logging.warning(...)` — pipeline continua |
| **Continuidade temporal** | `temporal_continuity_validation()` | `MAX_GAP_DAYS = 10` dias entre datas consecutivas | `logging.warning(...)` com as datas após cada lacuna — pipeline continua |
| **Range de valores** | `value_range_validation()` | `valor > 0` | `logging.warning(...)` com as datas dos registros inválidos — pipeline continua |

### Detalhes das Checagens

**Continuidade temporal** — detecta lacunas anormais na série histórica (ex: feriados prolongados, dados faltantes da API). Calcula a diferença em dias entre datas consecutivas e emite aviso para gaps superiores a 10 dias.

**Range de valores** — identifica registros com taxa SELIC igual a zero ou negativa, o que indica dado corrompido ou erro na API.

---

## 7. Como Executar Localmente

### Com Docker Compose (recomendado)

**Pré-requisitos:** Docker e Docker Compose instalados, mínimo 4 GB de RAM disponível para os containers.

```bash
# 1. Clone o repositório
git clone <url-do-repositorio>
cd selic-pipeline

# 2. Inicialize os serviços (build + start)
sh run.sh
# Ou, equivalentemente:
docker compose up -d

# 3. Aguarde a inicialização (aprox. 1-2 minutos) e acesse a UI
# URL: http://localhost:8080
# Usuário: airflow | Senha: airflow
```

**Para acionar a DAG:**

1. Acesse `http://localhost:8080` e faça login.
2. Localize a DAG `Selic` e clique em **"Trigger DAG w/ config"** (ícone de play com engrenagem).
3. Uma janela será exibida com os campos `date_start` e `date_end`. Informe o período desejado no formato `DD/MM/AAAA`:

| Campo | Descrição | Exemplo |
|-------|-----------|---------|
| `date_start` | Data de início da consulta | `01/01/2022` |
| `date_end` | Data de fim da consulta | `31/12/2023` |

4. Clique em **"Trigger"** para executar.

**Para encerrar os serviços:**

```bash
docker compose down
```

---


## 8. Parâmetros Configuráveis

| Parâmetro | Descrição | Valor padrão | Como configurar |
|-----------|-----------|--------------|-----------------|
| `date_start` | Data de início da consulta à API BCB | `01/01/2020` | DAG Param — campo JSON no "Trigger DAG w/ config" |
| `date_end` | Data de fim da consulta à API BCB | `31/12/2024` | DAG Param — campo JSON no "Trigger DAG w/ config" |
| `AIRFLOW_DATA_PATH` | Diretório raiz para armazenamento de dados (Bronze/Silver/Gold) | `./data` | Variável de ambiente no `.env` ou `docker-compose.yaml` |
| `AIRFLOW_UID` | UID do usuário para permissões de volume no Linux | `50000` | Variável de ambiente no `.env` |
| `retries` | Número de retentativas automáticas por task em caso de falha | `3` | `default_args` na DAG (`dags/selic.py`) |
| `retry_delay` | Intervalo entre retentativas | `1 minuto` | `default_args` na DAG (`dags/selic.py`) |
| `MIN_RECORDS` | Volume mínimo de registros esperado na camada Silver | `100` | Constante em `include/silver/selic_task2.py` |
| `MAX_GAP_DAYS` | Máximo de dias consecutivos sem dados antes de emitir aviso | `10` | Constante em `include/silver/selic_task2.py` |

---

## 9. Dependências

| Biblioteca | Versão | Descrição |
|-----------|--------|-----------|
| `pandas` | 2.3.3 | Manipulação e transformação de dados tabulares (leitura de Parquet, agregações, conversão de tipos) |
| `requests` | 2.34.2 | Requisições HTTP para consumo da API do Banco Central do Brasil |
| `pyarrow` | 24.0.0 | Leitura e escrita de arquivos Parquet de forma eficiente na camada Bronze |
| `psycopg2-binary` | 2.9.10 | Driver PostgreSQL para a conexão do Airflow com o banco de metadados |
| `openpyxl` | 3.1.5 | Exportação de DataFrames para formato `.xlsx` na camada Gold |

---

## 10. Observações Finais

### Limitações Conhecidas

- **Execução manual apenas:** a DAG está configurada com `schedule=None`, sem agendamento automático. Para execução periódica, basta definir uma expressão cron no parâmetro `schedule` da DAG.
- **Acumulado anual parcial:** o cálculo de `taxa_acumulada_anual` inclui os meses disponíveis no período consultado. Para um ano incompleto, o valor refletirá apenas os meses presentes nos dados.
- **Sem particionamento dos dados Gold:** o arquivo `selic_metrics.parquet` é sobrescrito a cada execução. Para manter histórico de múltiplas execuções, seria necessário adicionar particionamento por data de carga.

### Pontos de Atenção

- O arquivo `.env` contém `AIRFLOW_UID`, necessário para evitar problemas de permissão nos volumes Docker no Linux. Em caso de erro de permissão ao iniciar os containers, execute `echo "AIRFLOW_UID=$(id -u)" >> .env`.
- A API do BCB pode apresentar instabilidade ocasional. O mecanismo de retry (3 tentativas, 1 minuto de intervalo) mitiga falhas transitórias, mas não garante recuperação em janelas de indisponibilidade prolongadas.
 