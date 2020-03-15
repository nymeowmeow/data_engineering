from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
REDSHIFT_CONN_ID = 'redshift'
AWS_CREDENTIALS_ID = 'aws_credentials'
S3_BUCKET          = "udacity-dend"
LOG_DATA           = 'log_data'
SONG_DATA          = 'song_data'
REGION             = "us-west-2"

default_args = {
    'owner'       : 'udacity',
    'start_date'  : datetime(2020, 3, 14),
    'retries'     : 3,
    'retry_delay' : timedelta(minutes=5),
    'catchup'     : False,
    'depends_on_past' : False
}

dag = DAG('etl_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs = 1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id = REDSHIFT_CONN_ID,
    aws_credentials_id = AWS_CREDENTIALS_ID,
    table = "staging_events",
    s3_bucket = S3_BUCKET,
    s3_key = LOG_DATA,
    region = REGION,
    json_path = "s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id = REDSHIFT_CONN_ID,
    aws_credentials_id = AWS_CREDENTIALS_ID,
    table = "staging_songs",
    s3_bucket = S3_BUCKET,
    s3_key = SONG_DATA,
    region = REGION
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = REDSHIFT_CONN_ID,
    table = "songplays",
    sql   = SqlQueries.songplay_table_insert,
    overwrite=True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = REDSHIFT_CONN_ID,
    table = "users",
    sql = SqlQueries.user_table_insert,
    overwrite=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = REDSHIFT_CONN_ID,
    table = "songs",
    sql = SqlQueries.song_table_insert,
    overwrite=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = REDSHIFT_CONN_ID,
    table = "artists",
    sql = SqlQueries.artist_table_insert,
    overwrite=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = REDSHIFT_CONN_ID,
    table = "time",
    sql = SqlQueries.time_table_insert,
    overwrite=True,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = REDSHIFT_CONN_ID,
    tables = [ "songplays", "users", "songs", "artists", "time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
# setup dependency
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator

