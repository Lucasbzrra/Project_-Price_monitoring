# Projeto: Monitoramento de Preços de Produtos de Hardware

## Descrição do Projeto

Este projeto implementa um pipeline de dados funcional usando **Airflow** e a DAG **DagWebScraping** para um processo completo de ETL (Extração, Transformação e Carga). Ele integra múltiplas fontes de dados e utiliza **Python**, **Selenium**, **BeautifulSoup** e **Programação Orientada a Objetos** para automatizar a extração e manipulação de dados sobre produtos de hardware.

## Estrutura do Pipeline de Dados

### 🔍 Etapa de Extração

- **Objetivo**: Extrair dados de diversos sites de e-commerce sobre produtos de hardware, como processadores, placas-mãe, memórias e placas de vídeo.
- **Ferramentas e Técnicas**:
  - Desenvolvi **operadores personalizados** no Airflow, utilizando `SparkSubmitOperator`, `PythonOperator` e `BashOperator`.
  - A **classe `OperatorSiteBase`** foi criada para padronizar a extração de dados de diferentes fontes.
  - Dados extraídos são salvos em um **Data Lake** com três camadas:
    - **Raw Zone**: Dados brutos.
    - **Silver Zone**: Dados transformados e limpos.
    - **Gold Zone**: Dados prontos para análise.

### ⚙️ Etapa de Transformação

- **Objetivo**: Estruturar os dados em um modelo relacional.
- **Ferramentas e Técnicas**:
  - Utilização do **SparkSubmitOperator** para transformar dados da Raw Zone em tabelas de dimensões e fatos.
  - Dados finais são armazenados no **Data Lake** usando **Amazon S3** via **MinIO**.

### 📊 Monitoramento e Validação

- **Objetivo**: Garantir a integridade dos dados e o sucesso das operações do pipeline.
- **Ferramentas e Técnicas**:
  - **Sensores** monitoram o status da Silver Zone no Airflow.
  - **BranchOperator** valida condições específicas e envia alertas por e-mail se necessário.

### 📥 Etapa de Carga

- **Objetivo**: Carregar dados transformados no PostgreSQL.
- **Ferramentas e Técnicas**:
  - Uma DAG dependente conecta-se ao banco de dados **PostgreSQL**.
  - Cria tabelas, relacionamentos e insere dados usando o operador Spark.

## Tecnologias Utilizadas

- **Docker**: Orquestração dos containers de Airflow, Spark, PostgreSQL e MinIO.
- **Airflow**: Orquestração e agendamento de tarefas ETL.
- **Python**: Desenvolvimento de operadores e classes personalizadas.
- **Spark**: Transformação de dados e estruturação.
- **PostgreSQL**: Armazenamento de dados transformados.
- **MinIO**: Simulação do S3 para o Data Lake.

## Estrutura do Projeto

```plaintext
├── dags/
|   ├── LoadWebScraping.py
│   └── DagWebScraping.py          
├── operators/
|       ├──OperatorBase.py
│       ├── OperatorSiteA.py
|       ├── OperatorSiteB.py
|       ├── OperatorFileSensor.py       
│       └── OperatorSpark.py      
├── src/
│   └── scripts_spark/
│       ├── LoadFileToDB.py       
│       └── WebScraping_Transformation.py 
├── minio_data/
│   ├── rawzone/                   
│   ├── silver/                    
│   └── gold/                      
├── docker-compose.yaml            
├── Dockerfile
├── Dockerfile.spark                     
└── README.md                      
