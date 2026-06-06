from airflow.sdk import dag,task

@dag(
    dag_id="xcom_auto_dag",
)

def xcom_auto_dag():

    @task
    def first_task():
        print("Extracting Data ...")
        fetched_data = {"data": [1,2,3,4,5]}
        return fetched_data
    
    @task
    def second_task(fetched_data:dict):
        print("Transforming Data ...")
        transformed_data = [i*2 for i in fetched_data["data"]]
        transformed_data_dict = {"data": transformed_data}
        return transformed_data_dict
    
    @task
    def third_task(transformed_data:dict):
        print("Loading Data ...")
        print(f"Data to be loaded: {transformed_data['data']}")
        return transformed_data
    
    first = first_task()
    second = second_task(first)
    third = third_task(second)  

    first >> second >> third

dag = xcom_auto_dag()