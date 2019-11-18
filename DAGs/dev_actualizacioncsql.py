# -*- coding: utf-8 -*-
"""
Created on Thu Aug 29 13:06:20 2019

@author: dmaeztu
"""

import os
from datetime import datetime, timedelta
import dev_config as cfg

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator
from airflow.contrib.operators.gcp_sql_operator import CloudSqlInstanceImportOperator

default_args = {
    'start_date': datetime(2019, 8, 30, 0, 0), 'retries': 0,
    'retry_delay': timedelta(minutes=5), 'project_id': cfg.PROJECT_ID,
    'region': cfg.REGION}

NAME = os.path.basename(__file__).replace('.py', '').split("_")
ENV, OBJECT = NAME[0].upper(), NAME[-1]

# kwargs
BQ_PROJECT = ''  # TODO: Bigquery billing project
SQL_PROJECT = ''  # TODO: cloud Sql project
DATABASE = ''  # TODO: Cloud Sql Schema
TABLE = ''  # TODO: Cloud Sql table
DIR_TMP = ''  # TODO: Cloud Storage temporal directory
# TODO: Use your own SQL query inserted between (""" """)
QUERY = """
SELECT
FROM ``
"""

with DAG(ENV + "_" + OBJECT.upper(), schedule_interval=None, default_args=default_args,
         template_searchpath=''.format(cfg.ENV, OBJECT)) as dag:  # TODO: Template_searchpath

    create_tmp_table = BigQueryOperator(
        task_id='create_tmp_table',
        sql=QUERY,
        use_legacy_sql=False,
        destination_dataset_table='{}.Temporal.cloudSQLexport_tmp'.format(cfg.PROJECT_ID),
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id=cfg.bigquery_conn_id)
    
    create_tmp_csv = BigQueryToCloudStorageOperator(
            task_id='create_tmp_csv',
            source_project_dataset_table='{}.Temporal.cloudSQLexport_tmp'.format(BQ_PROJECT),
            destination_cloud_storage_uris='{}/cloudSQLexport_temp.csv'.format(DIR_TMP),
            export_format='CSV',
            print_header=False,
            bigquery_conn_id=cfg.bigquery_conn_id
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

    delete_tmp_table = BigQueryTableDeleteOperator(
            task_id='delete_tmp_table',
            deletion_dataset_table='{}.Temporal.cloudSQLexport_tmp'.format(BQ_PROJECT),
            bigquery_conn_id=cfg.bigquery_conn_id
            )

    delete_tmp_csv =  BashOperator(
        task_id='delete_tmp_csv',
        bash_command='gsutil rm {}/cloudSQLexport_temp.csv'.format(DIR_TMP)
        )
    
    # Dependencies between tasks
    create_tmp_table >> create_tmp_csv >> import_to_csql
    create_tmp_csv >> delete_tmp_table
    import_to_csql >> delete_tmp_csv
