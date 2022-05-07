from influxdb_client import Point, InfluxDBClient
from typing import Dict, List

from influxdb_client.client.write_api import ASYNCHRONOUS

from race_strategist.config import InfluxDBConfiguration
from race_strategist.connectors.egress.influxdb.processor import InfluxDBProcessor


class InfluxDBConnector:
    _processor: InfluxDBProcessor

    def __init__(self, configuration: InfluxDBConfiguration) -> None:

        self.config = configuration
        self._connection = None
        self._write_api = None

    @property
    def processor(self) -> InfluxDBProcessor:
        if not self._processor:
            self._processor = InfluxDBProcessor()
        return self._processor

    @property
    def connection(self) -> InfluxDBClient:
        if not self._connection:
            self._connection = InfluxDBClient(
                url=self.config.host, token=self.config.token
            )
        return self._connection

    @property
    def write_api(self):
        if not self._write_api:
            self._write_api = self.connection.write_api(write_options=ASYNCHRONOUS)
        return self._write_api

    def record_pulse(self, reading: Dict):
        return
        # if reading:
        #     print(f'{reading}')
        #     data.append(
        #         # f"health,tag=pulse pulse={reading['bpm']}"
        #         f'health,track={race_details.circuit},'
        #         f'lap={lap_number},session_uid={race_details.session_uid},'
        #         f'session_type={race_details.session_type},'
        #         f"stat=pulse pulse={reading['bpm']}"
        #     )
        # influx_conn.write(data)

    def write(self, data: List[str]):
        """
        data = [
            "mem,host=host1 used_percent=23.43234543",
            "mem,host=host1 available_percent=15.856523"
            ]
        A list of strings, the format for the data is:
        name,tag_name=tag_value field=values

        name - the high level name, in the above case memory
        tag_name - the list of tags
        field - name of the field and the value

        Example:
            "car_status,circuit=monza,lap=3,race_type=championship speed=287"
        """

        # explore:
        # write_api = client.write_api(write_options=ASYNCHRONOUS)
        #
        # _point1 = Point("my_measurement").tag("location", "Prague").field("temperature",
        #                                                                   25.3)
        # _point2 = Point("my_measurement").tag("location", "New York").field(
        #     "temperature", 24.3)
        #
        # async_result = write_api.write(bucket="my-bucket", record=[_point1, _point2])
        # async_result.get()
        #
        # client.close()
        # or
        # with _client.write_api(write_options=WriteOptions(batch_size=500,
        #                                                       flush_interval=10_000,
        #                                                       jitter_interval=2_000,
        #                                                       retry_interval=5_000,
        #                                                       max_retries=5,
        #                                                       max_retry_delay=30_000,
        #                                                       exponential_base=2))
        #                                                       as _write_client:
        # see https://github.com/influxdata/influxdb-client-python

        # write_api = self.connection.write_api(write_options=SYNCHRONOUS)
        self.write_api.write(self.config.bucket, self.config.org, data)
        # async_result.get()
