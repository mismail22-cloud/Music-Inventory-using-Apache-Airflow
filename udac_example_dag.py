from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime.now(),
    'catchup' : False,
    'depends_on_past' : False,
    'retries': 3,
    'retry_delay' : timedelta(minutes=5),
    'email_on_retry' : False
}

    
        
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          start_date=default_args.get('start_date'),
          schedule_interval='@hourly',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_tables
)

    
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    aws_credentials="aws_credentials",
    redshift_conn_id="redshift" ,
    file_source_path="s3://udacity-dend/log_data",
    destination_table="public.staging_events" ,
    JSON_format="JSON {}".format("'s3://udacity-dend/log_json_path.json'"),
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    aws_credentials="aws_credentials",
    redshift_conn_id="redshift" ,
    file_source_path="s3://udacity-dend/song_data" ,
    destination_table="public.staging_songs" ,
    JSON_format="format as json {}".format("'auto'"),
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift" ,
    insert_statement=SqlQueries.songplay_table_insert ,
    destination_table="public.songplays" ,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift" ,
    insert_statement=SqlQueries.user_table_insert ,
    destination_table="public.users" ,
    load_mode='Truncate_Load',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift" ,
    insert_statement=SqlQueries.song_table_insert ,
    destination_table="public.songs" ,
    load_mode='Truncate_Load',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift" ,
    insert_statement=SqlQueries.artist_table_insert ,
    destination_table="public.artists" ,
    load_mode='Truncate_Load',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift" ,
    insert_statement=SqlQueries.time_table_insert ,
    destination_table='public."time"' ,
    load_mode='Truncate_Load',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift" ,
    test_cases=[{'sql_check': "SELECT COUNT(*) FROM users WHERE userid is null", 'expct_rslt': 0}],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> create_tables_task
create_tables_task >> stage_events_to_redshift
create_tables_task >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator

