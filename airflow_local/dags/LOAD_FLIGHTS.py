# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import pathlib
from datetime import datetime, timedelta
import yaml

# Permitir import jobs.* desde airflow_local/jobs
BASE_DIR = pathlib.Path(__file__).resolve().parents[1]  # .../airflow_local
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from airflow import DAG
from airflow.operators.python import PythonOperator

# Callables de tus scripts
from jobs.extract import extract as job_extract        # -> list[dict]
from jobs.transform import transform as job_transform  # (rows) -> list[dict]
from jobs.load import upsert_rows as job_load          # (rows) -> None

# ---- Cargar YAML y castear lo mínimo ----
CFG_PATH = BASE_DIR / "config" / "dag_config.yaml"
cfg = {}
if CFG_PATH.exists():
    with open(CFG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

default_args = cfg.get("default_args", {}) or {}
# retry_delay_min -> timedelta
retry_delay_min = int(default_args.pop("retry_delay_min", 1))
default_args["retry_delay"] = timedelta(minutes=retry_delay_min)

dag_args = cfg.get("dag_args", {}) or {}
# start_date ISO -> datetime
sd = dag_args.get("start_date", "2025-01-01")
dag_args["start_date"] = datetime.fromisoformat(str(sd))
# schedule_interval: aceptar null/"None"/"null"
if str(dag_args.get("schedule_interval")).lower() in {"none", "null"}:
    dag_args["schedule_interval"] = None

with DAG(default_args=default_args, **dag_args) as dag:
    t_extract = PythonOperator(
        task_id="extract",
        python_callable=job_extract,
    )

    t_transform = PythonOperator(
        task_id="transform",
        python_callable=job_transform,
        op_kwargs={"rows": t_extract.output},
    )

    t_load = PythonOperator(
        task_id="load",
        python_callable=job_load,
        op_kwargs={"rows": t_transform.output},
    )

    t_extract >> t_transform >> t_load
