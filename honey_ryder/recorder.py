import json
import logging
import os
import sys
from typing import List, NamedTuple, Union

from honey_ryder.config import RecorderConfiguration
from honey_ryder.connectors.heart_beat_monitor import SerialSensor, _detect_port
from honey_ryder.connectors.influxdb import InfluxDBConnector
from honey_ryder.connectors.kafka import KafkaConnector
from honey_ryder.telemetry.constants import PACKET_MAPPER, SESSION_TYPE, TRACK_IDS
from honey_ryder.telemetry.listener import TelemetryFeed

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


class Race(NamedTuple):
    circuit: str
    session_type: str
    session_link_identifier: int


class DataRecorder:
    _kafka: Union[KafkaConnector, None] = None
    _kafka_unavailable: bool = False
    _influxdb: Union[InfluxDBConnector, None] = None

    def __init__(self, configuration: RecorderConfiguration, port: int = 20777) -> None:
        self.configuration: RecorderConfiguration = configuration
        self.feed = TelemetryFeed(port=port)
        self.port = port

    @property
    def kafka(self):
        if not self._kafka and self.configuration.kafka and not self._kafka_unavailable:
            self._kafka = KafkaConnector(configuration=self.configuration.kafka)
            if not self._kafka.in_error:
                self._kafka_unavailable = True
        return self._kafka

    @property
    def influxdb(self):
        if not self._influxdb and self.configuration.influxdb:
            self._influxdb = InfluxDBConnector(
                configuration=self.configuration.influxdb
            )

        return self._influxdb

    def write_to_influxdb(self, data: List) -> bool:
        if not self.influxdb:
            return False

        self.influxdb.write(data)
        return True

    def get_heart_rate(self):
        sensor_reader = SerialSensor(port=_detect_port())
        return sensor_reader.read()

    def write_to_file(self) -> bool:
        # with open('/Volumes/WORK MAC/F1_2021_Data/full_data.json', 'w') as \
        #         data_output:
        #     with open('/Volumes/WORK MAC/F1_2021_Data/team_data.json',
        #               'w') as team_output:
        #         data_output.write('[')
        #         team_output.write('[')
        #         try:
        #             while True:
        #                 packet, team_data = self.feed.get_latest()
        #                 #   file_output.write('cheese')
        #
        #                 if packet:
        #                     data_output.write(json.dumps(packet))
        #                     data_output.write(',\n')
        #                 if team_data:
        #                     team_output.write(json.dumps(team_data))
        #                     team_output.write(',\n')
        #             #
        #             # if not packet:
        #             #     continue
        #         except KeyboardInterrupt:
        #             print('Interrupted')
        #             data_output.write(']')
        #             team_output.write(']')
        return False

    def listen(self):
        race_details = None
        logger.info(f'Starting server to receive telemetry data on port: {self.port}.')
        current_lap_number = 1

        while True:
            packet, team_data = self.feed.get_latest()

            if not team_data:
                continue

            influxdb_data = []
            kafka_data = {}
            player = team_data['player_data']
            teammate = team_data['teammate_data']

            if team_data['type'] == 'PacketSessionData' and not race_details:
                race_details = Race(
                    circuit=TRACK_IDS[player['track_id']],
                    session_type=SESSION_TYPE[player['session_type']],
                    session_link_identifier=player['session_link_identifier'],
                )
                continue

            if team_data['type'] not in PACKET_MAPPER.keys():
                continue

            if team_data['type'] == 'PacketLapData':
                current_lap_number = player['current_lap_num']

            if self.influxdb:
                self.write_to_influxdb(player)
                self.write_to_influxdb(teammate)
        #
        #

        #
        # # we are late, so spin until we find out which race we are at
        # if not race_details:
        #     continue
        #
        # if packet['type'] == 'PacketLapData_V1':
        #     lap_number = int(packet['currentLapNum'])
        #
        # if packet['type'] in PACKET_MAPPER.keys():
        #     packet_name: str = 'unknown'
        #
        #     for name, value in packet.items():
        #
        #         if name == 'name':
        #             packet_name = value
        #
        #         if name in ['type', 'mfdPanelIndex', 'buttonStatus', 'name']:
        #             continue
        #
        #         # drivers name are bytes
        #         if name == 'name' and packet['type'] == 'PacketParticipantsData_V1':
        #             if type(value) == bytes:
        #                 value = value.decode('utf-8')
        #
        #         # FIXME
        #         if name not in ['sessionUID', 'team_name']:
        #             if self.influxdb:
        #                 influxdb_data.append(
        #                     f'{packet_name},track={race_details.circuit},'
        #                     f'lap={lap_number},'
        #                     f'session_uid={race_details.session_uid},'
        #                     f'session_type={race_details.session_type}'
        #                     f' {name}={value}'
        #                 )
        #
        #             if self.kafka:
        #                 kafka_data = self.kafka.build_data(
        #                     name=name, value=value, data=kafka_data
        #                 )
        #
        # if self.kafka:
        #     kafka_msg = {
        #         'lap_number': lap_number,
        #         'circuit': race_details.circuit,
        #         'session_uid': race_details.session_uid,
        #         'session_type': race_details.session_type,
        #         'data': kafka_data,
        #     }
        #     if packet_name:
        #         self.kafka.send(packet_name, json.dumps(kafka_msg).encode('utf-8'))
        #
        # if self.influxdb:
        #     self.influxdb.write(influxdb_data)
