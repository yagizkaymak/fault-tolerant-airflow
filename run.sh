#!/bin/bash

airflow resetdb
airflow initdb
yes | cp -rf src/dags/PDS_DAG.py /home/ubuntu/airflow/dags
cd src/ft_airflow
