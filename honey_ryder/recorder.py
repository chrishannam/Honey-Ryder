import logging
from typing import List, Union, Dict

from telemetry_f1_2021.packets import HEADER_FIELD_TO_PACKET_TYPE

from honey_ryder.config import RecorderConfiguration
# from honey_ryder.connectors.heart_beat_monitor import SerialSensor, _detect_port
from honey_ryder.connectors.influxdb.influxdb import InfluxDBConnector
from honey_ryder.connectors.kafka import KafkaConnector
from honey_ryder.session.session import Race
from honey_ryder.telemetry.constants import SESSION_TYPE, TRACK_IDS, DRIVERS, TEAMS
from honey_ryder.telemetry.listener import TelemetryFeed
from honey_ryder.connectors.influxdb.formatters.packet_format import formatter

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


class DataRecorder:
    _kafka: Union[KafkaConnector, None] = None
    _kafka_unavailable: bool = False
    _influxdb: Union[InfluxDBConnector, None] = None
    player_car_index = -1
    teammate_car_index = -1

    player: Dict = {}
    teammate: Dict = {}

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

    # def get_heart_rate(self):
    #     sensor_reader = SerialSensor(port=_detect_port())
    #     return sensor_reader.read()

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

    def _prepare_data(self, packet):
        data = packet.to_dict()
        header = data['m_header']

        key = (header['m_packet_format'], header['m_packet_version'],
               header['m_packet_id'])

        packet_type_class = HEADER_FIELD_TO_PACKET_TYPE[key]
        data_type = packet_type_class.__name__

        # setup car's data location in the array of all cars.
        # setup details about who the player is driving for and their teammate

        player_data = {}
        teammate_data = {}

        if data_type == 'PacketParticipantsData' and self.player_car_index == -1:
            if self.player_car_index == -1:
                self.player_car_index = header['m_player_car_index']

            self.player = data['m_participants'][self.player_car_index]

            for i, racer in enumerate(data['m_participants']):
                if racer['m_team_id'] == self.player['m_team_id'] and \
                        racer['m_driver_id'] != self.player['m_driver_id']:
                    self.teammate = racer
                    self.teammate_car_index = i
                    break

        # spin until we get setup
        if not self.player:
            return {}

        if data_type not in ['PacketParticipantsData']:
            for name, value in data.items():

                # skip adding header again
                if name == 'header':
                    continue

                if isinstance(value, list) and len(value) == 22:
                    player_data |= value[self.player_car_index]
                    teammate_data |= value[self.teammate_car_index]
                else:
                    player_data[name] = value
                    teammate_data[name] = value

        return {
            'type': data_type,
            'player_data': player_data,
            'teammate_data': teammate_data,
        }

    def listen(self):
        session_details = None
        current_lap_number = None
        participants = None
        logger.info(f'Starting server to receive telemetry data on port: {self.port}.')

        while True:
            packet = self.feed.get_latest()

            if not packet:
                continue

            data = self._prepare_data(packet=packet)

            if not data:
                continue

            # player = data['player_data']
            # teammate = data['teammate_data']
            # data_type = data['type']
            #
            if data['type'] == 'PacketSessionData' and not session_details:
                session_details = Race(
                    circuit=TRACK_IDS[data['player_data']['m_track_id']],
                    session_type=SESSION_TYPE[data['player_data']['m_session_type']],
                    session_link_identifier=
                    data['player_data']['m_session_link_identifier'],
                )
                continue

            if data['type'] == 'PacketLapData':
                current_lap_number = data['player_data']['m_current_lap_num']

            if data['type'] == 'PacketParticipantsData' and not participants:
                participants = packet.to_dict()['m_participants']

                for driver in participants:
                    driver['name'] = DRIVERS[driver['m_driver_id']] if driver[
                                                                           'm_driver_id'] in DRIVERS else 'Unknown'
                    driver['team'] = TEAMS[driver['m_team_id']] if driver[
                                                                       'm_team_id'] in TEAMS else 'Unknown'

            if not current_lap_number or not self.player_car_index > -1 \
                    or not session_details:
                continue

            if self.influxdb:
                player_formatted = self.format_packet(
                    packet, self.player_car_index, session_details,
                    current_lap_number, participants)
                # teammate_formatted = influxdb_format_packet(
                #     packet, self.player_car_index, session_details, current_lap_number)
                if player_formatted:
                    self.write_to_influxdb(player_formatted)
                    # self.write_to_influxdb(teammate)
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

    def format_packet(self, packet, player_car_index, session_details,
                      current_lap_number, participants):

        return formatter(packet, player_car_index, session_details,
                  current_lap_number, participants)
