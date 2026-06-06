from airflow.sdk import dag,task

@dag(
    dag_id="xcom_kwargs_dag",
)

def xcom_kwargs_dag():
    
    @task
    def first_task(**kwargs):

        ti = kwargs["ti"]
    
        print("Extracting Data ...")
        fetched_data = {"data": [1,2,3,4,5]}
        ti.xcom_push(key='return_result', value=fetched_data)

    @task
    def second_task(**kwargs):

        ti = kwargs["ti"]

        print("Transforming Data ...")
        fetched_data = ti.xcom_pull(key='return_result', task_ids='first_task')
        transformed_data = [i*2 for i in fetched_data["data"]]
        transformed_data_dict = {"data": transformed_data}
        ti.xcom_push(key='return_result', value=transformed_data_dict)

    @task
    def third_task(**kwargs):

        ti = kwargs["ti"]

        print("Loading Data ...")
        transformed_data = ti.xcom_pull(key='return_result', task_ids='second_task')
        print(f"Data to be loaded: {transformed_data['data']}")
        return transformed_data

    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third

dag = xcom_kwargs_dag()  