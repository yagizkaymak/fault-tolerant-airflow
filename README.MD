# Table of Contents
1. [Author](README.MD#author)
1. [Introduction](README.MD#introduction)
1. [Motivation](README.MD#motivation)
1. [Directory Structure](README.MD#directory-structure)
1. [Fault-Tolerant Airflow](README.MD#fault-tolerant-airflow)
1. [Data Pipeline](README.MD#data-pipeline)


## Author
This code has been developed by Yagiz Kaymak in using Python 2.7 as an Insight Data Science project.
The code is also accessible online by using the following link:
https://github.com/yagizkaymak/fault-tolerant-airflow

February, 2019

## Introduction
This Python code implements a fault-tolerant Apache Airflow architecture and tested the failure scenarios on a
fully functional data pipeline consisting of AWS S3 -> Spark Batch Processing -> PostgreSQL.

Apache Airflow is a platform to programmatically author, schedule and monitor workflows (https://airflow.apache.org/).
Airflow can be used to automate any workflow (e.g., a data pipeline) by developing a Python script.
A workflow in Airflow is represented as a Directed Acyclic Graph (DAG) of tasks (https://en.wikipedia.org/wiki/Directed_acyclic_graph).
If one task (e.g. Task2) is dependent on the completion of another task (e.g., Task1) these tasks
can be represented as Task1 -> Task2 as a DAG. This means that Task2 can not run before the completion of Task1.

## Motivation

## Directory Structure
'src' directory includes the source files within three sub-directories as 'dags', 'ft_airflow', and 'spark'.


## Fault-Tolerant Airflow


## Data Pipeline
