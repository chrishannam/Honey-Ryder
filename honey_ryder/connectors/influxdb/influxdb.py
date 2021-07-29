"""
Connector for sending data to the time series database InfluxDB.

See - https://www.influxdata.com/
"""
from pathlib import Path
from typing import List, Dict

from influxdb_client.client.write_api import SYNCHRONOUS
from telemetry_f1_2021.packets import HEADER_FIELD_TO_PACKET_TYPE, PacketMotionData, \
    PacketCarTelemetryData, PacketParticipantsData, PacketEventData, PacketSessionData

from honey_ryder.config import InfluxDBConfiguration
from influxdb_client import InfluxDBClient
import logging

from honey_ryder.session.session import Race

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)


class InfluxDBConnector:
    def __init__(self, configuration: InfluxDBConfiguration) -> None:

        self.config = configuration
        self._connection = None
        self._write_api = None

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
            self._write_api = self.connection.write_api(write_options=SYNCHRONOUS)
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


def influxdb_format_packet(data, index: int, race: Race, lap: int) -> Dict:
    """
    index is the position in list of the car we are interested in.
    """
    formatted = {}

    if data.__class__.__name__ in ['PacketParticipantsData',
                                   'PacketSessionHistoryData']:
        return {}
        # return _format_participants_packet(data, index, race, lap)
    elif data.__class__.__name__ == 'PacketSessionData':
        return _format_session_packet(data, index, race, lap)
    elif data.__class__.__name__ == 'PacketEventData':
        return _format_event_packet(data, index, race, lap)
    else:
        return _format_grid_packet(data, index, race, lap)

    return formatted


def _format_event_packet(data: PacketEventData, index: int, race: Race, lap: int,
                        log_player_only=True):
    """
        Grid packet has all 22 cars available data.
    """

    formatted_data = []
    header = None
    key = None
    packet_type = None

    for key, value in data.to_dict().items():
        if key == 'm_header':
            header = value
            key = (header['m_packet_format'],
                   header['m_packet_version'], header['m_packet_id'])
            packet_type = HEADER_FIELD_TO_PACKET_TYPE[key].__name__
        elif key == 'm_fastest_lap':
            formatted_data.append(
                f'{packet_type.replace("Packet", "")},'
                f'track={race.circuit},lap={lap},'
                f'session_uid={race.session_link_identifier},'
                f'session_type={race.session_type} fastest_lap='
                f'{value["m_fastest_lap"]["m_lap_time"]}'
            )
        else:
            continue
    return formatted_data


def _format_session_packet(data: PacketSessionData, index: int, race: Race, lap: int,
                        log_player_only=True):
    """
        Grid packet has all 22 cars available data.
    """

    formatted_data = []
    header = None
    key = None
    packet_type = None

    for key, value in data.to_dict().items():
        if key == 'm_header':
            header = value
            key = (header['m_packet_format'],
                   header['m_packet_version'], header['m_packet_id'])
            packet_type = HEADER_FIELD_TO_PACKET_TYPE[key].__name__
        elif key in ['m_weather_forecast_samples', 'm_marshal_zones']:
            continue
        else:
            formatted_data.append(
                f'{packet_type.replace("Packet", "")},'
                f'track={race.circuit},lap={lap},'
                f'session_uid={race.session_link_identifier},'
                f'session_type={race.session_type} {key}={value}'
            )
    return formatted_data


def _format_grid_packet(data: PacketMotionData, index: int, race: Race, lap: int,
                        log_player_only=True):
    """
        Grid packet has all 22 cars available data.
    """

    formatted_data = []
    header = None
    key = None
    packet_type = None

    for key, value in data.to_dict().items():
        if key == 'm_header':
            header = value
            key = (header['m_packet_format'],
                   header['m_packet_version'], header['m_packet_id'])
            packet_type = HEADER_FIELD_TO_PACKET_TYPE[key].__name__
        elif isinstance(value, list) and len(value) == 22:
            if log_player_only:
                player = value[index]
                for sub_key, sub_value in player.items():
                    if isinstance(sub_value, list) and len(sub_value) == 4:
                        # each corner of the car in this order RL, RR, FL, FR
                        for i, corner in enumerate(['rl', 'rr', 'fl', 'fr']):
                            end = 'front' if corner.startswith('f') else 'rear'
                            formatted_data.append(
                                f'{packet_type.replace("Packet", "")},'
                                f'track={race.circuit},lap={lap},'
                                f'session_uid={race.session_link_identifier},'
                                f'session_type={race.session_type},corner={corner},'
                                f'area_of_car={end}'
                                f' {sub_key}={sub_value[i]}'
                            )
                    else:
                        formatted_data.append(
                            f'{packet_type.replace("Packet", "")},'
                            f'track={race.circuit},lap={lap},'
                            f'session_uid={race.session_link_identifier},'
                            f'session_type={race.session_type}'
                            f' {sub_key}={sub_value}'
                        )
            else:
                for racer in value:
                    for top_level_key, top_level_value in racer.items():
                        formatted_data.append(
                            f'{packet_type.replace("Packet", "")},'
                            f'track={race.circuit},lap={lap},'
                            f'session_uid={race.session_link_identifier},'
                            f'session_type={race.session_type}'
                            f' {top_level_key}={top_level_value}'
                        )
        elif isinstance(value, list) and len(value) == 4:
            # each corner of the car in this order RL, RR, FL, FR
            for i, corner in enumerate(['rl', 'rr', 'fl', 'fr']):
                end = 'front' if corner.startswith('f') else 'rear'
                formatted_data.append(
                    f'{packet_type.replace("Packet", "")},'
                    f'track={race.circuit},lap={lap},'
                    f'session_uid={race.session_link_identifier},'
                    f'session_type={race.session_type},corner={corner},'
                    f'area_of_car={end}'
                    f' {key}={value[i]}'
                )
        else:
            formatted_data.append(
                f'{packet_type.replace("Packet", "")},'
                f'track={race.circuit},lap={lap},'
                f'session_uid={race.session_link_identifier},'
                f'session_type={race.session_type} {key}={value}'
            )
    return formatted_data


def _format_participants_packet(data: PacketParticipantsData, index: int, race: Race,
                             lap: int):
    """
    """

    formatted_data = []

    for key, value in data.to_dict().items():
        if key == 'm_header':
            continue
        elif key.endswith('_participants'):
            player = value[index]
            for top_level_key, top_level_value in player.items():
                formatted_data.append(
                    f'participants,track={race.circuit},lap={lap},'
                    f'session_uid={race.session_link_identifier},'
                    f'session_type={race.session_type}'
                    f' {top_level_key}={top_level_value}'
                )
        else:
            formatted_data.append(
                f'participants,track={race.circuit},lap={lap},'
                f'session_uid={race.session_link_identifier},'
                f'session_type={race.session_type} {key}={value}'
            )
    return formatted_data

