from  pathlib import Path
from sys import exception
import psycopg2
import pandas as pd
from sqlalchemy  import create_engine, engine,text
import shutil
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

def csv_to_postgres():
    engine=create_engine('postgresql://postgres:postgres@host.docker.internal:5432/demo')


    folder=Path('/opt/airflow/files/new')
    dfolder=Path('/opt/airflow/files/old')

    dfolder.mkdir(parents=True,exist_ok=True)

    if not folder.exists():
        raise FileNotFoundError("Folder Not Found")
    else:
        for f in folder.iterdir():
         if f.is_file():
            try:
                logging.info('File is Processing')

                df=pd.read_csv(f)
                # df=df.duplicated(keep='first')
                df=df.dropna(subset=["email"])
                tablename=f.stem.split("_")[0]
                with engine.begin() as conn:
                 df.to_sql(tablename,conn,if_exists='append',index=False)

                print("Data Inserted")

                shutil.move(f,dfolder / f.name)
                logging.info('File Moved Successfully')


            except Exception as e:
                logging.info(f"Duplicated Data in Error and file was not Moved :  {e}")
                raise 
        else:
                logging.info("File Not Found")

with DAG(
    dag_id='Csv_to_postgres',
    start_date=datetime(2026,1,1),
    schedule_interval="0 0 3 * *",
    catchup=False,
) as dag :

 run_etl=PythonOperator(
    task_id="run_etl",
    python_callable=csv_to_postgres
 )










