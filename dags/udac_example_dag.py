from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, CreateTablesRedshiftOperator)

from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2021, 11, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=1
        )
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_staging_events_table_task= CreateTablesRedshiftOperator(
        task_id="create_staging_events_table_task",
        dag=dag, 
        redshift_conn_id="redshift",
        sql_stmt=SqlQueries.staging_events_create_table
)

create_staging_songs_table_task = CreateTablesRedshiftOperator(
        task_id="create_staging_songs_table_task",
        dag=dag, 
        redshift_conn_id="redshift",
        sql_stmt=SqlQueries.staging_songs_create_table
)


load_stage_events_to_redshift = StageToRedshiftOperator(
    task_id='load_stage_events_to_redshift',
    dag=dag,
    table="public.staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log-data",
    region="us-west-2",
    extra_params="FORMAT AS JSON 's3://udacity-dend/log_json_path.json'"
)


load_stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='load_stage_songs_to_redshift',
    dag=dag,
    table="public.staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song-data",
    region="us-west-2",
    extra_params="JSON 'auto' COMPUPDATE OFF"
)


create_artists_table_task = CreateTablesRedshiftOperator(
        task_id="create_artists_table_task",
        dag=dag, 
        redshift_conn_id="redshift",
        sql_stmt=SqlQueries.artists_create_table
)

create_songplays_table_task = CreateTablesRedshiftOperator(
        task_id="create_songplays_table_task",
        dag=dag, 
        redshift_conn_id="redshift",
        sql_stmt=SqlQueries.songplays_create_table
)

create_songs_table_task = CreateTablesRedshiftOperator(
        task_id="create_songs_table_task",
        dag=dag, 
        redshift_conn_id="redshift",
        sql_stmt=SqlQueries.songs_create_table
)

create_users_table_task = CreateTablesRedshiftOperator(
        task_id="create_users_table_task",
        dag=dag, 
        redshift_conn_id="redshift",
        sql_stmt=SqlQueries.users_create_table
)

create_time_table_task = CreateTablesRedshiftOperator(
        task_id="create_time_table_task",
        dag=dag, 
        redshift_conn_id="redshift",
        sql_stmt=SqlQueries.time_create_table
)


load_songplays_table = LoadFactOperator(
    task_id='load_songplays_table',
    dag=dag,
    table='songplays',
    redshift_conn_id="redshift",
    sql_stmt=SqlQueries.songplay_table_insert
)


load_user_dimension_table = LoadDimensionOperator(
    task_id='load_user_dimension_table',
    dag=dag,
    table='users',
    redshift_conn_id="redshift",
    sql_stmt=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='load_song_dimension_table',
    dag=dag,
    table='songs',
    redshift_conn_id="redshift",
    sql_stmt=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='load_artist_dimension_table',
    dag=dag,
    table='artists',
    redshift_conn_id="redshift",
    sql_stmt=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='load_time_dimension_table',
    dag=dag,
    table='time',
    redshift_conn_id="redshift",
    sql_stmt=SqlQueries.time_table_insert
)

data_quality_checks_task = DataQualityOperator(
    task_id='data_quality_checks_task',
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks=[
        { 'check_sql': 'SELECT COUNT(*) FROM public."time" WHERE weekday IS NULL', 'expected_result': 0 },
        { 'check_sql': 'SELECT COUNT(*) FROM public.songplays WHERE userid IS NULL', 'expected_result': 0 }, 
        { 'check_sql': 'SELECT COUNT(DISTINCT "gender") FROM public.users', 'expected_result': 2 },
        { 'check_sql': 'SELECT COUNT(*) FROM public.artists WHERE name IS NULL', 'expected_result': 0 },
        { 'check_sql': 'SELECT COUNT(*) FROM public.users WHERE first_name IS NULL', 'expected_result': 0 },
        { 'check_sql': 'SELECT COUNT(*) FROM public.songs WHERE title IS NULL', 'expected_result': 0 },
        { 'check_sql': 'SELECT COUNT(DISTINCT "level") FROM public.songplays', 'expected_result': 2 } 
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# STEP 1
start_operator >> create_staging_events_table_task
start_operator >> create_staging_songs_table_task

# # STEP 2
create_staging_events_table_task >> load_stage_events_to_redshift
create_staging_songs_table_task >> load_stage_songs_to_redshift

# STEP 
load_stage_events_to_redshift >> create_songplays_table_task
load_stage_events_to_redshift >> create_artists_table_task 
load_stage_events_to_redshift >> create_songs_table_task 
load_stage_events_to_redshift >> create_users_table_task 
load_stage_events_to_redshift >> create_time_table_task
load_stage_songs_to_redshift >> create_songplays_table_task
load_stage_songs_to_redshift >> create_artists_table_task 
load_stage_songs_to_redshift >> create_songs_table_task
load_stage_songs_to_redshift >> create_users_table_task
load_stage_songs_to_redshift >> create_time_table_task


# #STEP 4
create_songplays_table_task >> load_songplays_table
create_artists_table_task >> load_songplays_table
create_songs_table_task >> load_songplays_table
create_users_table_task >> load_songplays_table

# STEP 5
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> data_quality_checks_task
load_song_dimension_table >> data_quality_checks_task
load_artist_dimension_table >> data_quality_checks_task
load_time_dimension_table >> data_quality_checks_task

data_quality_checks_task >> end_operator