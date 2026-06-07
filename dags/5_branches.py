from airflow.sdk import dag, task

@dag(
    dag_id='branch_dag',
)    
def branch_dag():

    @task
    def extract_task(**kwargs):
        print("Extracting Data ....")
        ti = kwargs['ti']
        extracted_data_dict = {
            'api_extracted_data':[1,2,3],
            'db_extracted_data':[4,5,6],
            's3_extracted_data':[7,8,9],
            'flag': 'false'
        }
        ti.xcom_push(key='return_value', value=extracted_data_dict)

    @task
    def transform_task_api(**kwargs):
        ti= kwargs['ti']
        api_extracted_data = ti.xcom_pull(task_ids='extract_task', key='return_value')['api_extracted_data']
        transformed_api_data = [i*10 for i in api_extracted_data]
        ti.xcom_push(key='return_value', value=transformed_api_data)

    @task
    def transform_task_db(**kwargs):
        ti= kwargs['ti']
        db_extracted_data = ti.xcom_pull(task_ids='extract_task', key='return_value')['db_extracted_data']
        transformed_db_data = [i*100 for i in db_extracted_data]
        ti.xcom_push(key='return_value', value=transformed_db_data)

    @task
    def transform_task_s3(**kwargs):
        ti= kwargs['ti']
        s3_extracted_data = ti.xcom_pull(task_ids='extract_task', key='return_value')['s3_extracted_data']
        transformed_s3_data = [i*1000 for i in s3_extracted_data]
        ti.xcom_push(key='return_value', value=transformed_s3_data)

    @task.branch
    def check_flag(**kwargs):
        ti = kwargs['ti']
        flag_value = ti.xcom_pull(task_ids='extract_task', key='return_value')['flag']
        if flag_value == 'true':
            return 'skip_load_task'
        else:
            return 'load_task'

    @task.bash
    def load_task(**kwargs):
        print("Loading data to destination ....")
        api_data = kwargs['ti'].xcom_pull(task_ids='transform_task_api')
        db_data = kwargs['ti'].xcom_pull(task_ids='transform_task_db')
        s3_data = kwargs['ti'].xcom_pull(task_ids='transform_task_s3')

        return f"echo 'Loaded Data:{api_data}, {db_data}, {s3_data}'"
    
    @task.bash
    def skip_load_task(**kwargs):
        return "echo 'Skipping Load Task'"

    extract= extract_task()
    transform_api = transform_task_api()
    transform_db = transform_task_db()
    transform_s3 = transform_task_s3()
    check_flag = check_flag()
    load = load_task()
    skip_load = skip_load_task()

    extract >> [transform_api,transform_db, transform_s3] >> check_flag >> [load, skip_load]

dag = branch_dag()