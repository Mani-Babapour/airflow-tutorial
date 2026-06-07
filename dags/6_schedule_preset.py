from airflow.decorators import dag, task
from pendulum import datetime

@dag(
    dag_id="first_schedule_dag",
    start_date=datetime(2026, 6, 6, tz="Asia/Tehran"),
    schedule="@daily",
    is_paused_upon_creation=False,
    catchup=True
)
def first_schedule_dag():

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


dag = first_schedule_dag()