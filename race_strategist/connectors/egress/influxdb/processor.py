
from influxdb_client import Point, InfluxDBClient
from typing import Dict, List

from influxdb_client.client.write_api import ASYNCHRONOUS

from config import InfluxDBConfiguration
from race_strategist.modelling.processor import Processor
from race_strategist.session.session import Driver, CurrentLaps


class InfluxDBProcessor(Processor):

    def convert(self, data: Dict, packet_name: str):
        data_name = packet_name.replace('Packet', '').replace('Data', '').replace('Car', '')

        if packet_name in ['PacketCarSetupData', 'PacketMotionData',
                           'PacketCarDamageData', 'PacketCarTelemetryData',
                           'PacketCarStatusData']:
            return self.extract_car_array_data(packet=data, data_name=data_name)
        elif packet_name == 'PacketLapData':
            return self._process_laps(laps=data, data_name=data_name)
        elif packet_name == 'PacketSessionData':
            return self._process_session(session=data, data_name=data_name)
        elif packet_name == 'PacketSessionHistoryData':
            return self._process_session_history(session=data, data_name=data_name)

    def _process_session_history(self, session: Dict, data_name: str):
        driver = self.drivers.drivers[session['car_idx']]
        points = []

        for name, value in session.items():
            if name in ['header', 'car_idx', 'num_laps']:
                continue
            if isinstance(value, float) or isinstance(value, int):
                points.append(
                    self.create_point(
                        packet_name=data_name,
                        key=name,
                        value=value,
                        lap=self.current_lap.current_lap_num,
                        driver=driver,
                        team=driver.team_name
                    )
                )
            elif isinstance(value, list):
                for index, lap_data in enumerate(value):
                    if name == 'lap_history_data':
                        if lap_data['lap_time_in_ms'] != 0:
                            for k, v in lap_data.items():
                                # 0x01 bit set-lap valid,
                                # 0x02 bit set-sector 1 valid
                                # 0x04 bit set-sector 2 valid
                                # 0x08 bit set-sector 3 valid

                                points.append(
                                    self.create_point(
                                        packet_name=data_name,
                                        key=k,
                                        value=v,
                                        lap=self.current_lap.current_lap_num,
                                        driver=driver,
                                        team=driver.team_name
                                    )
                                )
                    elif name == 'tyre_stints_history_data':
                        for k, v in lap_data.items():
                            if k == 'end_lap' and v == 0:
                                break

                            points.append(
                                self.create_point(
                                    packet_name=data_name,
                                    key=k,
                                    value=v,
                                    lap=self.current_lap.current_lap_num,
                                    driver=driver,
                                    team=driver.team_name
                                )
                            )
        return points

    def _process_session(self, session: Dict, data_name: str):
        points = []
        for name, value in session.items():
            if name == 'header':
                continue
            if isinstance(value, float) or isinstance(value, int):
                points.append(
                    self.create_point(
                        packet_name=data_name,
                        key=name,
                        value=value,
                        lap=self.current_lap.current_lap_num
                    )
                )
            elif isinstance(value, list):
                if name == 'weather_forecast_samples':
                    continue
                else:
                    for i in value:
                        if isinstance(i, dict):
                            for k, v in i.items():
                                points.append(
                                    self.create_point(
                                        packet_name=data_name,
                                        key=k,
                                        value=v,
                                        lap=self.current_lap.current_lap_num
                                    )
                                )
        return points

    def _process_laps(self, laps: Dict, data_name: str):
        points = []
        for driver_index, lap in enumerate(laps['lap_data']):
            if driver_index >= len(self.drivers.drivers):
                continue

            for name, value in lap.items():

                if name == 'current_lap_num':
                    pass
                driver = self.drivers.drivers[driver_index]
                lap_number: int = lap['current_lap_num']
                points.append(
                    self.create_point(
                        packet_name=data_name,
                        key=name,
                        value=value,
                        lap=lap_number,
                        driver=driver,
                        team=driver.team_name
                    )
                )
        return points

    def update_laps(self, laps: CurrentLaps):
        self.laps = laps

    def create_point(self, packet_name: str, key: str, value: float, lap: int, driver: Driver = None, team: str = None,
                     tags: Dict = None) -> Point:

        if tags is None:
            tags = dict()

        point = Point(packet_name).tag('circuit', self.session.circuit) \
            .tag('session_uid', self.session.session_link_identifier) \
            .tag('session_type', self.session.session_type) \
            .tag('lap', lap) \
            .field(key, value)

        if driver:
            point.tag('driver_name', driver.driver_name)
        if team:
            point.tag('team', team)

        for tag_name, tag_value in tags.items():
            point.tag(tag_name, tag_value)

        return point

    def extract_car_array_data(self, packet: Dict, data_name: str):
        points = []

        lap_number = self.laps.laps[packet['header']['player_car_index']].current_lap_num
        driver = self.drivers.drivers[packet['header']['player_car_index']]

        for name, value in packet.items():
            if name == 'header':
                continue

            if isinstance(value, list) and len(value) == 4:
                for location, corner in enumerate(['rear_left', 'rear_right', 'front_left', 'front_right']):
                    points.append(
                        self.create_point(
                            packet_name=data_name,
                            key=name,
                            value=round(value[location], 6),
                            tags={'corner': corner},
                            lap=lap_number,
                            driver=driver,
                            team=driver.team_name
                        )
                    )
            elif isinstance(value, list):
                for idx, data in enumerate(packet[list(packet.keys())[1]]):
                    if idx >= len(self.drivers.drivers):
                        continue

                    for name, value in data.items():
                        driver = self.drivers.drivers[idx]
                        if isinstance(value, list) and len(value) == 4:
                            for location, corner in enumerate(['rear_left', 'rear_right', 'front_left', 'front_right']):
                                points.append(
                                    self.create_point(
                                        packet_name=data_name,
                                        key=name,
                                        value=value[location],
                                        tags={'corner': corner},
                                        lap=lap_number,
                                        driver=driver,
                                        team=driver.team_name
                                    )
                                )
                        elif isinstance(value, list):
                            pass
                        else:
                            points.append(
                                self.create_point(
                                    packet_name=data_name,
                                    key=name,
                                    value=value,
                                    lap=lap_number,
                                    driver=driver,
                                    team=driver.team_name
                                )
                            )
            else:
                points.append(
                    self.create_point(
                        packet_name=data_name,
                        key=name,
                        value=value,
                        lap=lap_number,
                        driver=driver,
                        team=driver.team_name
                    )
                )
        return points


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
