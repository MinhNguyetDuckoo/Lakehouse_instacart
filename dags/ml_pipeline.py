from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="ml_training_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    check_data = BashOperator(
        task_id="check_gold_data",
        bash_command="ls /opt/airflow/data/gold"
    )

    train_model = BashOperator(
        task_id="train_als",
        bash_command="""
        spark-submit \
        --master local[*] \
        /opt/airflow/ml/train_als.py
        """
    )

    done = BashOperator(
        task_id="done",
        bash_command="echo 'Model updated successfully'"
    )

    check_data >> train_model >> done