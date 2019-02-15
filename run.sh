#!/bin/bash

yes | airflow resetdb
airflow initdb
yes | cp -rf src/dags/PDS_DAG.py /home/ubuntu/airflow/dags
yes | cp -rf /home/ubuntu/Projects/PDS/src/main/python/secret.py /home/ubuntu/fault-tolerant-airflow/src/dags
yes | cp -rf /home/ubuntu/Projects/PDS/src/main/python/secret.py /home/ubuntu/fault-tolerant-airflow/src/spark
yes | cp -rf /home/ubuntu/Projects/PDS/src/main/python/secret.py /home/ubuntu/fault-tolerant-airflow/src/ft_airflow
