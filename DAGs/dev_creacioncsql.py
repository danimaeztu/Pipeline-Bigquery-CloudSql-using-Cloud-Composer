# -*- coding: utf-8 -*-
"""
Created on Thu Aug 29 13:06:20 2019

@author: dmaeztu
"""

import os
from datetime import datetime, timedelta

import pandas as pd
import gcsfs  # Used by pandas to access the Cloud Storage
import dev_config as cfg

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcp_sql_operator import CloudSqlInstanceImportOperator, CloudSqlQueryOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.operators.mysql_operator import MySqlOperator

default_args = {
    'start_date': datetime(2019, 8, 30, 0, 0), 'retries': 0,
    'retry_delay': timedelta(minutes=5), 'project_id': cfg.PROJECT_ID,
    'region': cfg.REGION}

NAME = os.path.basename(__file__).replace('.py', '').split("_")
ENV, OBJECT = NAME[0].upper(), NAME[-1]

# kwargs
BQ_PROJECT = ''  # TODO: bigquery billing project
SQL_PROJECT = ''  # TODO: Cloud Sql project
DATABASE = ''  # TODO: Cloud Sql Schema
TABLE = ''  # TODO: Cloud Sql Table
DIR_TMP = ''  # TODO: Cloud Storage temporal directory
# TODO: Use your own SQL query inserted between (""" """)
QUERY = """
SELECT
FROM ``
"""


def get_df_and_types(**context):
    '''Set the columns and types of a csv to create a table with SQL.
    Save a CSV to import later'''
    def traducir(x):
        '''Translate panda's type to SQL type'''
        tipoSQL = []
        tr = ''
        for i in x:
            if i == 'float64':
                tr = 'float'
            elif i == 'int64':
                tr = 'int'
            else:
                tr = 'text'
            tipoSQL.append(str(tr))
        return(tipoSQL)

    bq = BigQueryHook(bigquery_conn_id=cfg.bigquery_conn_id,
                      use_legacy_sql=False)

    df = bq.get_pandas_df(QUERY)
    df.to_csv('{}/cloudSQLexport_temp.csv'.format(DIR_TMP), index=None, header=False)

    df = pd.DataFrame(df.dtypes)
    df.columns = ['tipo']
    df['nombre'] = df.index
    df['tipoSQL'] = traducir(df['tipo'])
    df['col_sql'] = df.apply(lambda x: "{} {} NULL".format(x['nombre'], x['tipoSQL']), axis=1)

    sqlcolumnas = ", ".join(df['col_sql'])
    context['ti'].xcom_push(key='SQL', value="CREATE TABLE {} ({})".format(TABLE, sqlcolumnas))


with DAG(ENV + "_" + OBJECT.upper(), schedule_interval=None, default_args=default_args,
         template_searchpath=''.format(cfg.ENV, OBJECT)) as dag:  # TODO: Template_searchpath

    get_df_and_types = PythonOperator(
        task_id='get_df_and_types',
        python_callable=get_df_and_types,
        provide_context=True
    )

    creartabla = CloudSqlQueryOperator(
           gcp_cloudsql_conn_id='cloudsql_pipeline',
           task_id="creartabla",
           sql="{{ti.xcom_pull(task_ids='get_df_and_types',key='SQL')}}"
           )

    import_to_csql = CloudSqlInstanceImportOperator(
            task_id='import_to_csql',
            project_id=SQL_PROJECT,
            instance='servicedat-cal-mysql',
            body={
            "importContext": {
                    "kind": "sql#importContext",
                    "fileType": 'CSV',
                    "uri": '{}/cloudSQLexport_temp.csv'.format(DIR_TMP),
                    "database": DATABASE,
                    "csvImportOptions": {
                            "table": TABLE
                            }
                    }
            },
            api_version='v1beta4',
            gcp_conn_id='cloudsql_pipeline'
            )

    delete_tmp_csv = BashOperator(
        task_id='delete_tmp_csv',
        bash_command='gsutil rm {}/cloudSQLexport_temp.csv'.format(DIR_TMP)
        )

    delete_xcom_task = MySqlOperator(
        task_id='delete-xcom-task',
        mysql_conn_id='airflow_db',
        sql="delete from xcom where dag_id='{}'".format(dag.dag_id))

    # Dependencies between tasks
    get_df_and_types >> creartabla >> import_to_csql >> delete_tmp_csv
    creartabla >> delete_xcom_task
