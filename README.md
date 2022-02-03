# ETL com Webscraping em um Catálogo de Produtos

As empresas precisam constantemente acompanhar os seus concorrentes e todos os SKU's deles.

A tarefa pode custar muito tempo e dinheiro para grandes empresas. 
Então, com uso de Webscraping e ETL bem estruturado (DAG) e documentados, podemos coletar todos esse catálogo automaticamente e lançá-los em um banco de dados para podermos utilizar posteriormente em análise de dados e de Business Intelligence (BI).

## Para resolver esse problema, utilizei: 
* Biblioteca **BealtifulSoup**, que consegue fazer essa **webscraping em páginas HTML** para coleta de informações.
* Técnicas de **manipulação e limpeza dos dados** através de bibliotecas como **Pandas e Numpy**.
* **Crontab**, que é o **scheduler** que executa o script com o ETL pipeline todo dia às 18h e às 6h.
* Biblioteca **SQLAlchemy** para gerenciar e enviar os dados coletados para um **banco de dados SQLite3**.
* Biblioteca **Logging** para **reportar todos os logs** (INFO, DEBUG, WARNING, ERROR, CRITICAL) da execução dos jobs para um txt.

###### **Observação:** O ETL do projeto é relativamente simples, com poucos jobs sem necessidade de execução em paralelo e com volume de dados reduzido. Caso necessário uma estrutura mais robusta para realização do pipeline, seria recomendado o uso de plataformas como o Airflow ao invés do Crontab.
