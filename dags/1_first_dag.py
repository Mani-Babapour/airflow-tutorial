from airflow.decorators import dag, task

@dag(
    dag_id="first_dag",
)
def first_dag():

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


dag = first_dag()