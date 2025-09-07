# Takeome DE - Fligoo

Orchestrate a pipeline to extract, transform, and load flight data from the Aviationstack API into PostgreSQL, using Apache Airflow.

    

## Objects:

- DAG: LOAD_FLIGHTS
- Scripts (called by the DAG): airflow_local/jobs/{extract.py, transform.py, load.py}
- DB: Postgres in Docker, initialized by init.sql (testdata table + unique index)

    

## Requirements

- Docker + Docker Compose
- Python 3.10
- Free ports:
  - 5432 (Postgres)
  - 8080 (Airflow webserver)
- API Key AVIATIONSTACK_KEY (configure your API Key in .env)

    

## Project structure:

.  
├─ airflow_local/  
│  ├─ dags/  
│  │  └─ LOAD_FLIGHTS.py  
│  ├─ jobs/  
│  │  ├─ __init__.py  
│  │  ├─ extract.py  
│  │  ├─ transform.py  
│  │  └─ load.py  
│  └─ requirements.txt   
├─ db/init/  
│  └─ init.sql  
├─ docker-compose.yml   
├─ .env  
└─ README.md  

    
    

# Installation (first time):

## 1) Create/Activate virtualenv
```bash
python3.10 -m venv .venv-airflow
source .venv-airflow/bin/activate
```

    

## 2) Install script dependencies
```bash
pip install --upgrade pip
pip install -r airflow_local/requirements.txt
```

    

## 3) Install Airflow (if not installed)
```bash
export AIRFLOW_VERSION=2.8.4
export PYTHON_VERSION=3.10
pip install "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
```

    

## 4) Export variables and AIRFLOW_HOME
```bash
set -a; source .env; set +a  
unset AIRFLOW_HOME AIRFLOW_CONFIG AIRFLOW__CORE__DAGS_FOLDER AIRFLOW__CORE__SQL_ALCHEMY_CONN AIRFLOW__LOGGING__BASE_LOG_FOLDER
export AIRFLOW_HOME="$PWD/airflow_local"  
rm -f "$AIRFLOW_HOME/airflow.db" \
      "$AIRFLOW_HOME/airflow.cfg" \
      "$AIRFLOW_HOME/webserver_config.py" \
      "$AIRFLOW_HOME/airflow-webserver.pid"
rm -rf "$AIRFLOW_HOME/logs"
export AIRFLOW__CORE__DAGS_FOLDER="$AIRFLOW_HOME/dags"
export AIRFLOW__LOGGING__BASE_LOG_FOLDER="$AIRFLOW_HOME/logs"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$AIRFLOW_HOME/airflow.db"
export AIRFLOW__WEBSERVER__WEB_SERVER_MASTER_TIMEOUT=300
export AIRFLOW__WEBSERVER__WEB_SERVER_WORKER_TIMEOUT=300 
```

    

## 5) Initialize Airflow DB (first time only)
```bash
airflow db init
```

    

## 6) Create Airflow admin user (first time only)
```bash
airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin 
```

    

## 7) Start Postgres (Docker) and create table
```bash
docker compose up -d  
docker compose cp db/init/init.sql postgres:/tmp/init.sql  
docker compose exec postgres bash -lc 'until pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"; do sleep 1; done'
docker compose exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /tmp/init.sql
```

    


# Start (every time)
You’ll need two terminals (one for the webserver and one for the scheduler).

## Terminal 1 - Webserver
```bash
cd /project/directory  
source .venv-airflow/bin/activate  
set -a; source .env; set +a  
export AIRFLOW_HOME="$PWD/airflow_local"  
airflow webserver -p 8080  
```

    

## Terminal 2 - Scheduler
```bash
cd /project/directory  
source .venv-airflow/bin/activate  
set -a; source .env; set +a    
export AIRFLOW_HOME="$PWD/airflow_local"  
airflow scheduler  
```

    

# Running the DAG

- Open Airflow UI: http://localhost:8080 (user: admin, pass: admin, by default)
- Find the DAG LOAD_FLIGHTS
- If it’s paused, Unpause it
- Trigger a run
- Inspect logs for tasks: extract → transform → load

    

# Verify database:
```bash
docker exec -it fligoo-postgres \
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT COUNT(*) AS total FROM testdata;"
```
