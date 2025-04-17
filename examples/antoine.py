import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    "antoine_dag",
    schedule=None,
    start_date=(pendulum.datetime(2024, 12, 1, tz="UTC")),
):
    BashOperator(
        task_id="extract",
        bash_command="touch 'hello world' && date && sleep 5",
        cwd=".",
    )

    BashOperator(
        task_id="transform",
        bash_command="sleep 1",
        cwd=".",
    )

    BashOperator(
        task_id="load",
        bash_command="true",
        cwd=".",
    )
