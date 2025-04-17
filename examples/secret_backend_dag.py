from collections.abc import AsyncIterator
from typing import Any

from airflow import DAG
from airflow.decorators import task
from airflow.sdk import BaseSensorOperator, Connection, Context, Variable
from airflow.triggers.base import BaseTrigger, TriggerEvent


def test_get_variable():
    assert Variable.get("my_variable") == "my_value"


def test_get_connection():
    assert (
        Connection.get("my_connection").get_uri()
        == "scheme://my_login:my_password@my_host:5432/my_schema"
    )


class VariableTrigger(BaseTrigger):
    def serialize(self) -> tuple[str, dict[str, Any]]:
        return ("secret_backend_dag.VariableTrigger", {})

    async def run(self) -> AsyncIterator[TriggerEvent]:
        test_get_variable()
        yield TriggerEvent({})


class ConnectionTrigger(BaseTrigger):
    def serialize(self) -> tuple[str, dict[str, Any]]:
        return ("secret_backend_dag.ConnectionTrigger", {})

    async def run(self) -> AsyncIterator[TriggerEvent]:
        test_get_connection()
        yield TriggerEvent({})


class VariableSensor(BaseSensorOperator):
    def execute(self, context: Context) -> None:
        self.defer(trigger=VariableTrigger(), method_name="execute_complete")

    def execute_complete(
        self, context: Context, event: dict[str, Any] | None = None
    ) -> None:
        return


class ConnectionSensor(BaseSensorOperator):
    def execute(self, context: Context) -> None:
        self.defer(trigger=ConnectionTrigger(), method_name="execute_complete")

    def execute_complete(
        self, context: Context, event: dict[str, Any] | None = None
    ) -> None:
        return


with DAG("secret_backend_dag"):

    @task()
    def test_get_variable_worker():
        test_get_variable()

    @task()
    def test_get_connection_worker():
        test_get_connection()

    test_get_variable_worker()
    test_get_connection_worker()

    VariableSensor(task_id="test_get_variable_triggerer")
    ConnectionSensor(task_id="test_get_connection_triggerer")

    test_get_variable()  # test_get_variable_dag_processor
    test_get_connection()  # test_get_connection_dag_processor
