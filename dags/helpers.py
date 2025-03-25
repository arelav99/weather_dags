from datetime import datetime
import json

def process_geo_coords(previous_task, **kwargs):
    geo_response = kwargs["ti"].xcom_pull(previous_task)
    if geo_response is not None:
        geo_json = json.loads(geo_response)
        return geo_json[0]["lat"], geo_json[0]["lon"]
    else:
        raise AttributeError('No value for coordinates has been given')

def process_weather_data(previous_task, **kwargs):
    geo_response = kwargs["ti"].xcom_pull(previous_task)
    if geo_response is not None:
        geo_json = json.loads(geo_response)
        current_weather = geo_json["data"][0]
        return (
            datetime.fromtimestamp(current_weather["dt"]).isoformat(),
            current_weather["temp"],
            current_weather["humidity"],
            current_weather["clouds"],
            current_weather["wind_speed"]
        )
    else:
        raise AttributeError('No value for weather info has been given')

def get_stage_name(base: str, city: str):
    return f"{base}_{city}"

def get_jinja_stage_name(base: str, city: str, inx: str):
    return "{{ti.xcom_pull(task_ids='" +\
            get_stage_name(base, city) +\
             f"')[{inx}]}}}}"

def get_airflow_attribute(attr: str):
    return "{{" + attr + "}}"

def push_unixtimestamp_of_current_run(**kwargs):
    execution_date = kwargs['execution_date']
    
    return (int(execution_date.timestamp()), 0)