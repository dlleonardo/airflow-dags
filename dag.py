from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta

# Task 1.1 - Define DAG arguments
dag_args = {
    'owner':'Leonardo',
    'start_date': days_ago(0),
    'email': ['leonardo@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Task 1.2 - Define the DAG
dag = DAG(
    dag_id = 'ETL_toll_data',
    schedule_interval = timedelta(days=1),
    default_args = dag_args,
    description = 'Apache Airflow Final Assignment'
)

# Task 1.3 - Create a task to unzip data
unzip_data = BashOperator(
    task_id = 'unzip_data',
    bash_command = 'sudo tar -zxvf /home/project/airflow/dags/finalassignment/tolldata.tgz',
    dag = dag
)

# Task 1.4 - Create a task to extract data from csv file
# extracting rowid, timestamp, anonymized vehicle number, vehicle type
extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv',
    bash_command = 'cut -d"," -f1,2,3,4 /home/project/airflow/dags/finalassignment/vehicle-data.csv | tr "," ";" > csv_data.csv'
    dag = dag
)

# Task 1.5 Create a task to extract data from tsv file
# extracting number of axles, tollplaza id, tollplaza code
extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv',
    bash_command = 'cut -f5,6,7 /home/project/airflow/dags/finalassignment/tollplaza-data.tsv | tr -s "\t" ";" > tsv_data.csv'
    dag = dag
)

# Task 1.6 - Create a task to extract data from fixed width file
#extracting type of payment code, vehicle code
extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    bash_command = 'cat /home/project/airflow/dags/finalassignment/payment-data.txt | tr -s " " | cut -d" " -f10,11 | tr " " ";" > fixed_width_data.csv'
    dag = dag
)

# Task 1.7 Create a task to consolidate data extracted from previous tasks
consolidate_data = BashOperator(
    task_id = 'consolidate_data,
    bash_command = 'paste csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    dag = dag
)

# Task 1.8 Transform the data
transform_data = BashOperator(
    task_id = 'transform_data',
    bash_command = 'cut -d";" -f4 extracted_data.csv | sed -e "s/^./\U&/g; s/ ./\U&/g" > ./staging/transformed_data.csv'
    dag = dag
)

# Task 1.9 Define the task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data

