from airflow import DAG
# from airflow.operators.sqlite_operator import SqliteOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.helpers import chain

from datetime import datetime


from helpers import (
    process_geo_coords,
    get_stage_name,
    process_weather_data,
    get_jinja_stage_name,
    push_unixtimestamp_of_current_run
)



CITIES = ["Lviv", "Kyiv", "Kharkiv", "Odesa", "Zhmerynka"]
# CITIES = ["Lviv"]

GET_UNIX_TIMESTAMP = "push_unixtimestamp_of_current_run"
GET_CITY_COORDS_STAGE_NAME = "get_coordinates"
PARSE_CITY_COORDS_STAGE_NAME = "parse_coordinates"
GET_CITY_WEATHER_STAGE_NAME = "get_weather"
PROCESS_WEATHER_STAGE_NAME = "process_weather"
INJECT_DATA_STAGE_NAME = "inject_data"

TIMESTAMP_ATTR = "ds"

with DAG(
    dag_id="simple_python_dag",
    start_date=datetime(2025, 1, 1),
    end_date=datetime.now(),
    catchup=True,
    schedule='@monthly'
) as dag:

    db_create = SQLExecuteQueryOperator(
        task_id="create_table_sqlite",
        conn_id="airflow_conn",
        sql="""
        CREATE TABLE IF NOT EXISTS measures
        (
        timestamp TIMESTAMP,
        temp FLOAT,
        humidity INT,
        clouds INT,
        wind_speed FLOAT
        );"""
    )

    get_run_date_unixtimestamp = PythonOperator(
        task_id=get_stage_name(GET_UNIX_TIMESTAMP, "all"),
        python_callable=push_unixtimestamp_of_current_run,
        provide_context=True
    )

    query_coordinates_tasks = [
        HttpOperator(
            task_id=get_stage_name(GET_CITY_COORDS_STAGE_NAME, city),
            http_conn_id="weather_conn",
            method="GET",
            endpoint="geo/1.0/direct",
            data={"q": city, "limit": "1", "appid": Variable.get('WEATHER_API_KEY')},
            response_check=lambda response: response.status_code == 200, 
        )
        for city in CITIES
    ]

    parse_coords_tasks = [
        PythonOperator(
            task_id=get_stage_name(PARSE_CITY_COORDS_STAGE_NAME, city),
            python_callable=process_geo_coords,
            provide_context=True,
            op_kwargs={'previous_task': get_stage_name(GET_CITY_COORDS_STAGE_NAME, city)},
        )
        for city in CITIES
    ]

    query_weather_tasks = [
        HttpOperator(
            task_id=get_stage_name(GET_CITY_WEATHER_STAGE_NAME, city),
            http_conn_id="weather_conn",
            endpoint="data/3.0/onecall/timemachine",
            data={
                "appid": Variable.get("WEATHER_API_KEY"),
                "lat": get_jinja_stage_name(PARSE_CITY_COORDS_STAGE_NAME, city, 0),
                "lon": get_jinja_stage_name(PARSE_CITY_COORDS_STAGE_NAME, city, 1),
                "dt": get_jinja_stage_name(GET_UNIX_TIMESTAMP, "all", 0)
            },
            method="GET",
            log_response=True
        )
        for city in CITIES
    ]

    process_data = [
        PythonOperator(
            task_id=get_stage_name(PROCESS_WEATHER_STAGE_NAME, city),
            python_callable=process_weather_data,
            op_kwargs={'previous_task': get_stage_name(GET_CITY_WEATHER_STAGE_NAME, city)},
        )
        for city in CITIES
    ]

    inject_data = [
        SQLExecuteQueryOperator(
            task_id=get_stage_name(INJECT_DATA_STAGE_NAME, city),
            conn_id="airflow_conn",
            sql=f"""
            INSERT INTO measures (timestamp, temp, humidity, clouds, wind_speed) VALUES
            (?, ?, ?, ?, ?);
            """,
            parameters=(
                get_jinja_stage_name(PROCESS_WEATHER_STAGE_NAME, city, 0),
                get_jinja_stage_name(PROCESS_WEATHER_STAGE_NAME, city, 1),
                get_jinja_stage_name(PROCESS_WEATHER_STAGE_NAME, city, 2),
                get_jinja_stage_name(PROCESS_WEATHER_STAGE_NAME, city, 3),
                get_jinja_stage_name(PROCESS_WEATHER_STAGE_NAME, city, 4)
            )
        )
        for city in CITIES
    ]

    db_create >> get_run_date_unixtimestamp >> query_coordinates_tasks
    chain(
        query_coordinates_tasks, 
        parse_coords_tasks, 
        query_weather_tasks, 
        process_data,
        inject_data
    )
