from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.interval import CronDataIntervalTimetable

@dag(
    dag_id="incremental_load_dag",
    start_date= datetime(year=2026, month=6, day=5, tz="Asia/Tehran"),
    end_date= datetime(year=2026, month=6, day=10, tz="Asia/Tehran"),
    schedule= CronDataIntervalTimetable("@daily", timezone="Asia/Tehran"),
    is_paused_upon_creation= False,
    catchup=True
)
def incremental_load_dag():

    @task
    def incremental_data_fetch(**kwargs):
        date_interval_start = kwargs['data_interval_start']
        date_interval_end = kwargs['data_interval_end']
        print(f"Fetching data from {date_interval_start} to {date_interval_end}")

    @task.bash
    def incremental_data_process():
        return "echo 'Processing data from {{data_interval_start}} to {{data_interval_end}}'"
    

    fetch_task = incremental_data_fetch()
    process_task = incremental_data_process()

    fetch_task >> process_task

dag = incremental_load_dag()