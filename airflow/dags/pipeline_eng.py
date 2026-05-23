from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

SCRIPT_LANDING_BRONZE = "/opt/airflow/scripts/landing_to_bronze.py"
SCRIPT_BRONZE_SILVER = "/opt/airflow/scripts/bronze_to_silver.py"
SCRIPT_BOOK_PEDIDOS = "/opt/airflow/scripts/book_pedidos.py"
#PYTHONPATH = "/opt/airflow:/opt/airflow/scripts:/opt/airflow/config"

#  load_task = BashOperator(
#        task_id="load_data",
#        bash_command=f"PYTHONPATH={PYTHONPATH} python {SCRIPT_LOAD}"
#    )

    # dbt_staging_task = BashOperator(
    #     task_id="dbt_run_staging",
    #     bash_command="cd /opt/airflow/dbt && dbt run --select staging"
    # )

with DAG(
    dag_id="elt_dag_pipeline",
    start_date=datetime(2026, 5, 9),
    schedule_interval="0 0 * * *",
    catchup=False,
    tags=["extract", "load", "transform"]
) as dag:

    landing_to_bronze_task = BashOperator(
        task_id="landing_to_bronze",
        bash_command=f"""
        export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
        export SPARK_HOME=/opt/spark
        export PATH=$SPARK_HOME/bin:$PATH
        export SPARK_LOCAL_IP=0.0.0.0

        spark-submit {SCRIPT_LANDING_BRONZE}
        """
    )
    
    
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"""
        export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
        export SPARK_HOME=/opt/spark
        export PATH=$SPARK_HOME/bin:$PATH
        export SPARK_LOCAL_IP=0.0.0.0

        spark-submit {SCRIPT_BRONZE_SILVER}
        """
    )
    

        

    landing_to_bronze_task >> bronze_to_silver 