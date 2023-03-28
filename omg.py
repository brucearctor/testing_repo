import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.plugins_manager import AirflowPlugin
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

id_index = None


class CBDruidIndexerSensor(BaseSensorOperator):
    template_fields = ["data_source"]
    ui_color = "#06914E"

    @apply_defaults
    def __init__(
        self,
        data_source,
        http_conn_id="http_druid",
        *args,
        **kwargs
    ):
        super(CBDruidIndexerSensor, self).__init__(*args, **kwargs)
        self.data_source = data_source
        self.http_conn_id = http_conn_id

    def poke(self, context):
        global id_index
        self.log.info("Checking the data source: %s", self.data_source)
        uri = BaseHook.get_connection(self.http_conn_id).host
        response = requests.get(
            "{URI}druid/indexer/v1/tasks?state=running".format(
                URI=uri
            ) 
        )

        for element in response.json():
            id_druid = element["id"]
            if "index_parallel_" in id_druid and self.data_source in id_druid:
                id_index = id_druid

        if id_index is not None:
            self.log.info(id_index)
            response = requests.get(
                "{URI}druid/indexer/v1/task/{druid_id}/status".format(
                    URI=uri, druid_id=id_index
                )
            )
            status_code = response.json()["status"]["statusCode"]
            if status_code in "RUNNING":
                return False
            elif status_code in "SUCCESS":
                return True
            elif status_code in "FAILED":
                raise AirflowException(
                    "Failed Druid import. Go to Druid UI to check the logs."
                )
        else:
            raise AirflowException("Failed to find running Druid tasks")

        return False


class CBDruidIndexerSensorPlugin(AirflowPlugin):
    name = "cbdruidindexer_plugin"
    operators = [CBDruidIndexerSensor]
