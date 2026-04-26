# Olist Lakehouse ML

Este projeto implementa um lakehouse de dados para análise de ML usando dados da Olist, com integração de Spark, MinIO, PostgreSQL e Jupyter.

## Pré-requisitos

- Python 3.8 ou superior
- Docker e Docker Compose
- Git

## Instalação e Configuração

### 1. Clone o repositório

```bash
git clone https://github.com/jeanlr/Machine-Learning-Olist.git
cd Machine-Learning-Olist
```

### 2. Configure o ambiente virtual Python

Crie e ative um ambiente virtual para desenvolvimento local:

```bash
python -m venv .venv
source .venv/bin/activate  # No Linux/Mac
# ou
.venv\Scripts\activate     # No Windows
```

Instale as dependências:

```bash
pip install -r requirements.txt
```

### 3. Configure as credenciais

Copie o arquivo de exemplo de variáveis de ambiente:

```bash
cp .env.example .env
```

Edite o arquivo `.env` e preencha as seguintes credenciais:

- `MINIO_ROOT_USER`: Usuário root do MinIO
- `MINIO_ROOT_PASSWORD`: Senha root do MinIO
- `POSTGRES_USER`: Usuário do PostgreSQL
- `POSTGRES_PASSWORD`: Senha do PostgreSQL
- `POSTGRES_DB`: Nome do banco de dados PostgreSQL
- `AIRFLOW_ADMIN_USERNAME`: Usuário admin do Airflow (se usar Airflow)
- `AIRFLOW_ADMIN_EMAIL`: Email do admin do Airflow
- `AIRFLOW_ADMIN_PASSWORD`: Senha do admin do Airflow
- `FERNET_KEY`: Chave Fernet para Airflow
- `SPARK_MASTER`: URL do Spark Master (geralmente spark://spark-master:7077)
- `SPARK_DRIVER_HOST`: Host do driver Spark

**Atenção:** Nunca commite o arquivo `.env` no Git. Ele contém informações sensíveis.

## Como subir o ambiente

O ambiente é orquestrado via Docker Compose. Para subir todos os serviços:

```bash
docker-compose up -d
```

Isso iniciará os seguintes serviços:

- **MinIO**: Armazenamento de objetos (porta 9000 para API, 9001 para console)
- **PostgreSQL**: Banco de dados (porta interna)
- **Spark Master**: Mestre do cluster Spark (porta 8083 para web UI, 7077 para conexões)
- **Spark Workers**: 3 workers do Spark
- **Jupyter**: Ambiente Jupyter Notebook (porta 8888)
- **MinIO Init**: Inicializa buckets no MinIO

### Acessando os serviços

- **Jupyter Notebook**: http://localhost:8888 (token vazio)
- **MinIO Console**: http://localhost:9001
- **Spark Master UI**: http://localhost:8083

### Parando o ambiente

```bash
docker-compose down
```

## Estrutura do Projeto

- `jupyter/`: Dockerfile e requirements para o ambiente Jupyter
- `spark/`: Dockerfile e configurações para o cluster Spark
- `scripts/`: Scripts Python para processamento de dados
- `docker-compose.yml`: Orquestração dos serviços

## Desenvolvimento

Para desenvolvimento local, ative o venv e use os scripts em `scripts/`. Para processamento distribuído, use o ambiente Docker.

## Contribuição

1. Fork o projeto
2. Crie uma branch para sua feature
3. Commit suas mudanças
4. Push para a branch
5. Abra um Pull Request

## Licença

Este projeto está sob a licença MIT.