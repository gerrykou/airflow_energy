# Readme
An app to monitor the task of downloading data to files from an API using Airflow

## Installing / Getting Started

Install docker compose   
https://docs.docker.com/compose/install/

### Initial Configuration

Inside the dags folder, add a config_local.py file that should contain url and credentials for the API

ENERGY_API_URL = 'your_url'   
ENERGY_API_UUID = 'your_uuid'   
ENERGY_API_TOKEN = 'your_token'

### Building
```shell
docker compose up airflow-init
docker compose up
```

run docker ps to find airflow-scheduler container id 
```shell
docker ps 
docker exec -it container-id bash
```   
https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html#backfill   
```shell
airflow dags backfill --start-date START_DATE --end-date END_DATE dag_id   
```   
e.g   
```shell
airflow dags backfill --start-date 2022-01-19 --end-date 2022-01-20 my_dag
```   

```shell
docker compose down
```

Visit localhost   
http://localhost:8080/


Readme template from   
https://github.com/jehna/readme-best-practices/blob/master/README-default.md

