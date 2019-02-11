# Table of Contents
1. [Author](README.MD#author)
1. [Introduction](README.MD#introduction)
1. [Motivation](README.MD#motivation)
1. [Fault-Tolerant Airflow](README.MD#fault-tolerant-airflow)
1. [Data Pipeline](README.MD#data-pipeline)
1. [Visualization](README.MD#visualization)
1. [Test Cases](README.MD#test-cases)
1. [Directory Structure](README.MD#directory-structure)



## Author
This code has been developed by Yagiz Kaymak using Python 2.7 as an Insight Data Science project.
The code is also accessible online as a Github repo:
https://github.com/yagizkaymak/fault-tolerant-airflow

February, 2019

## Introduction
This Python code implements a fault-tolerant Apache Airflow architecture and tested the failure scenarios on a
fully functional data pipeline consisting of AWS S3 -> Spark Batch Processing -> PostgreSQL.

Apache Airflow is a platform to programmatically author, schedule and monitor workflows (https://airflow.apache.org/).
Airflow can automate any workflow (e.g., a data pipeline) by using a Python script that
creates a Directed Acyclic Graph (DAG) (https://en.wikipedia.org/wiki/Directed_acyclic_graph) of tasks.
If one task (e.g. Task2) is dependent on the completion of another task (e.g., Task1) these tasks
can be represented as Task1 -> Task2 as a DAG. This means that Task2 can not run before the completion of Task1.

## Motivation
Apache Airflow employs and one or multiple workers that run the tasks of DAGs, and one scheduler
that orchestrates and schedules the tasks that are run by workers.
Airflow can be setup in single-node mode, where the scheduler and all workers run on the same machine.
Alternatively, Airflow can be setup in cluster mode, where the scheduler and workers can be distributed over
a number of instances/machines to provide fault tolerance for workers.
In both setup, there is __only one__ scheduler that orchestrates everything, which may be brittle for single point of
failure. In case of a scheduler failure, all workflows stall and a manual recovery is needed.

## Fault-Tolerant Airflow
Fault-tolerant Airflow architecture spins up two schedulers running on different machines to provide fault-tolerance.
One scheduler is in active and the other one is in standby mode to prevent duplicate job submissions.
These two fault-tolerant schedulers are synced via a periodic heartbeat message to see if the remote scheduler is active.
Both fault-tolerant schedulers are connected to a common database to simply store relevant information, such as active scheduler,
active backup scheduler, and the timestamp of the last heartbeat message.
If the active scheduler fails/crashes, the backup scheduler automatically takes over by detecting the absence of a heartbeat message
for sometime.
The fault-tolerant Airflow architecture provides fault-tolerance even if the machine that runs
the active scheduler crashes and becomes unresponsive.


## Data Pipeline
In order to test the Airflow-scheduler failure scenarios, a fully functional data pipeline that is
daily scheduled and orchestrated by the fault-tolerant Airflow is constructed.
The steps of the data pipeline are as follows:
*'Deutsche XETR Public Dataset' -> AWS S3 -> Spark Batch processing -> PostgreSQL*

This data pipeline is represented as a DAG in Airflow as follows:
*Ingest data from Deutsche XETR public dataset and store it in AWS S3 ->
Perform batch processing to calculate daily average stock prices and temporarily store the daily
average prices in a csv file in S3 ->
Detect if the daily csv file created and store the results in PostgreSQL if the file exists*

Note that each task (including the database store task) in the DAG is idempotent
(https://en.wikipedia.org/wiki/Idempotence),which means it can be repeated
without changing the result beyond the initial application.


## Visualization
The results of the calculated daily average stock prices are visualized on
__www.faulttolerantairflow.com__ using Dash app by Plotly.
On the same webpage, the status (i.e., ON - OFF) of the active and backup schedulers, and the
timestamp of the last heartbeat.
Moreover, the same web site has a button allowing to terminate/kill the active Airflow scheduler process.
Stopping the instance that runs the active scheduler is not implemented on the web site for security reasons.

## Test Cases
Two test cases exist to test the fault-tolerance of the developed system:
1. Airflow-scheduler process termination
2. Machine that runs Airflow scheduler crashes

The first test case can be run using the 'Terminate Active Scheduler' button on www.faulttolerantairflow.com.
The second test case requires to send a stop-instance signal to the working instance where the active scheduler
is running.


## Directory Structure
* 'src' directory includes the source files within three sub-directories as 'dags', 'ft_airflow', and 'spark'
  * 'dags' directory includes the DAG to be scheduled by Airflow
  * 'ft_airflow' directory includes the Python scripts for fault-tolerant airflow implementation
  * 'spark' directory includes the PySpark script for batch processing
* 'webapp' directory includes the Dash web app for visualization
