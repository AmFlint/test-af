from __future__ import annotations


import pendulum
from typing import Any
from airflow.models.dag import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.configuration import conf
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.base import BaseTrigger
from airflow.utils.context import Context


class FailExceptionTrigger(BaseTrigger):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def fall_on_face(self, context: Context) -> bool:
        raise Exception("This trigger should not be run")


class RaiseExceptionSensor(BaseSensorOperator):
    def __init__(
        self,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.deferrable = deferrable

    def execute(self, context: Context) -> None:
        self.defer(
            trigger=FailExceptionTrigger(),
            method_name="fall_on_face",
        )

    def execute_complete(
        self,
        context: Context,
        event: dict[str, Any] | None = None,
    ) -> None:
        # We have no more work to do here. Mark as complete.
        return


with DAG(
    dag_id="deferrable_fail_exception",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
    wait = RaiseExceptionSensor(task_id="wait")
    finish = EmptyOperator(task_id="finish")
    wait >> finish
