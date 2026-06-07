from airflow.decorators import dag, task
from pendulum import datetime
from airflow.timetables.trigger import CronTriggerTimetable

@dag(
    dag_id="cron_schedule_dag",
    start_date= datetime(2026, 6, 5, tz="Asia/Tehran"),
    schedule= CronTriggerTimetable("0 22 * * *", timezone="Asia/Tehran"),
    end_date= datetime(2026, 6, 10, tz="Asia/Tehran"),
    is_paused_upon_creation= False,
    catchup=True
)
def cron_schedule_dag():

    @task
    def first_task():
        print("This is the first task")

    @task
    def second_task():
        print("This is the second task")

    @task
    def third_task():
        print("This is the third task. Dag is complete!")

    @task
    def fourth_task():
        print("This is the fourth task. Dag is complete!")

    first = first_task()
    second = second_task()
    third = third_task()
    fourth = fourth_task()  

    first >> second >> third >> fourth


dag = cron_schedule_dag()