import logging
from typing import List, Union, Dict

from influxdb_client import Point
from telemetry_f1_2021.cleaned_packets import HEADER_FIELD_TO_PACKET_TYPE

from honey_ryder.config import RecorderConfiguration
# from honey_ryder.connectors.heart_beat_monitor import SerialSensor, _detect_port
from honey_ryder.connectors.influxdb.influxdb import InfluxDBConnector
from honey_ryder.connectors.kafka import KafkaConnector
from honey_ryder.session.session import Race
from honey_ryder.telemetry.constants import SESSION_TYPE, TRACK_IDS, DRIVERS, TEAMS, \
    WEATHER
from honey_ryder.telemetry.listener import TelemetryFeed
from honey_ryder.connectors.influxdb.formatters.packet_format import formatter
from honey_ryder.telemetry.session import Session

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
        self.participants = None

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

    def collect(self):
        BYPASS_PACKETS = ['PacketEventData']

        tags = {
            'session_uid': False,
            'circuit': False,
            'session_type': False
        }
        tags_filled = False

        laps = False
        while True:
            packet, packet_type = self.feed.get_latest()
            driver = packet.header.player_car_index

            # packet needs looping and writting to influxdb
            points = []

            header = packet.header
            packet_name = packet_type.__name__
            packet_dict = packet.to_dict()

            del packet_dict['header']

            # packets to avoid for now
            if packet_name in BYPASS_PACKETS:
                continue

            if not tags_filled:
                if packet_name == 'PacketSessionData':
                    tags['circuit'] = TRACK_IDS[packet.track_id]
                    tags['session_uid'] = header.session_uid
                    tags['session_type'] = SESSION_TYPE[packet_dict['session_type']]
                    tags_filled = True
                else:
                    continue

            if packet_name == 'PacketSessionData':
                points = extract_session_data(packet_dict, tags)

            if not self.participants:
                if packet_name != 'PacketParticipantsData':
                    continue
                # map drivers to positions in the array of 20
                self.participants = packet_dict['participants']
                continue

            if not laps:
                if packet_name == 'PacketLapData':
                    laps = packet_dict['lap_data']
                else:
                    continue

            # updates laps
            if packet_name == 'PacketLapData':
                laps = packet_dict['lap_data']
                points = extract_laps_data(packet_dict, self.participants, tags)

            elif packet_name in ['PacketCarSetupData', 'PacketMotionData',
                                 'PacketCarDamageData', 'PacketCarTelemetryData',
                                 'PacketCarStatusData']:
                points = extract_car_data(packet_dict, self.participants, tags, laps)

            if points:
                self.write_to_influxdb(points)


def extract_laps_data(packet: Dict, drivers, tags):
    points = []
    for idx, lap in enumerate(packet[list(packet.keys())[0]]):
        driver = drivers[idx]

        # check if this is the player
        if driver['driver_id'] not in DRIVERS:
            driver_name = driver['name']
        else:
            driver_name = DRIVERS[driver['driver_id']]

        if not lap or not driver_name:
            continue

        for key, value in lap.items():
            if key == 'current_lap_num':
                continue
            else:
                points.append(Point("LapData").tag('circuit', tags['circuit'])
                          .tag('session_uid', tags['session_uid'])
                          .tag('session_uid', tags['session_type'])
                          .tag('team', TEAMS[driver['team_id']])
                          .tag('lap', lap['current_lap_num'])
                          .tag('driver', driver_name)
                          .field(key, float(value))
                )
    return points


def extract_car_data(packet: Dict, drivers, tags, laps):
    points = []
    for idx, setup in enumerate(packet[list(packet.keys())[0]]):
        driver = drivers[idx]
        lap = laps[idx]

        # check if this is the player
        if driver['driver_id'] not in DRIVERS:
            driver_name = driver['name']
        else:
            driver_name = DRIVERS[driver['driver_id']]

        if not setup or not driver_name:
            continue

        for key, value in setup.items():
            if isinstance(value, list):
                # The order is as follows[RL, RR, FL, FR]
                # we have four things, usually tyres
                for location, corner in enumerate(['rl', 'rr', 'fl', 'fr']):
                    point = Point("CarSetup").tag('circuit', tags['circuit']) \
                        .tag('session_uid', tags['session_uid']) \
                        .tag('session_uid', tags['session_type']) \
                        .tag('team', TEAMS[driver['team_id']]) \
                        .tag('lap', lap['current_lap_num']) \
                        .tag('driver', driver_name) \
                        .field(key, float(value[location]))
                    if corner.startswith('r'):
                        point.tag('area_of_car', 'rear')
                    else:
                        point.tag('area_of_car', 'front')
                    point.tag('corner_of_car', corner)
            else:
                point = Point("CarSetup").tag('circuit', tags['circuit'])\
                              .tag('session_uid', tags['session_uid'])\
                              .tag('session_uid', tags['session_type'])\
                              .tag('team', TEAMS[driver['team_id']])\
                              .tag('lap', lap['current_lap_num'])\
                              .tag('driver', driver_name)\
                              .field(key, float(value))

            points.append(point)
    return points


def extract_session_data(packet: Dict, tags) -> List[Point]:
    """
    Not all the session data packet is needed, just extract the important stuff.
    INFO_DATA_STRING = '{packet_type},' \
               'circuit={circuit},lap={lap},' \
               'session_uid={session_link_identifier},' \
               'session_type={session_type},' \
               'team={team},' \
               'driver={driver}' \
               ' {metric_name}={metric_value}'
    """
    fields = [
        'track_temperature',
        'air_temperature',
        'pit_stop_rejoin_position',
        'pit_stop_window_ideal_lap',
        'pit_stop_window_latest_lap',
        'safety_car_status',
        'weather',
    ]
    points = []
    for key, value in packet.items():
        if key in fields:
            if key == 'weather':
                value = WEATHER[value]
                key = 'weather_condition'
            else:
                value = float(value)
            points.append(Point("SessionData").tag('circuit', tags['circuit'])
                          .tag('session_uid', tags['session_uid'])
                          .field(key, value))
    return points

    #     # def get_heart_rate(self):
    #     #     sensor_reader = SerialSensor(port=_detect_port())
    #     #     return sensor_reader.read()
    #
    #     def write_to_file(self) -> bool:
    #         # with open('/Volumes/WORK MAC/F1_2021_Data/full_data.json', 'w') as \
    #         #         data_output:
    #         #     with open('/Volumes/WORK MAC/F1_2021_Data/team_data.json',
    #         #               'w') as team_output:
    #         #         data_output.write('[')
    #         #         team_output.write('[')
    #         #         try:
    #         #             while True:
    #         #                 packet, team_data = self.feed.get_latest()
    #         #                 #   file_output.write('cheese')
    #         #
    #         #                 if packet:
    #         #                     data_output.write(json.dumps(packet))
    #         #                     data_output.write(',\n')
    #         #                 if team_data:
    #         #                     team_output.write(json.dumps(team_data))
    #         #                     team_output.write(',\n')
    #         #             #
    #         #             # if not packet:
    #         #             #     continue
    #         #         except KeyboardInterrupt:
    #         #             print('Interrupted')
    #         #             data_output.write(']')
    #         #             team_output.write(']')
    #         return False
    #
    #     def _extract_team_mate(self, packet):
    #         for i, racer in enumerate(packet.participants):
    #             if racer.team_id == self.player.team_id and \
    #                     racer.driver_id != self.player.driver_id:
    #                 self.teammate = racer
    #                 self.teammate_car_index = i
    #                 break
    #
    #     def _extract_player(self, packet):
    #         data = packet.to_dict()
    #         header = data['header']
    #
    #         key = (header['packet_format'], header['packet_version'],
    #                header['packet_id'])
    #
    #         packet_name = HEADER_FIELD_TO_PACKET_TYPE[key].__name__
    #
    #         # setup car's data location in the array of all cars.
    #         # setup details about who the player is driving for and their teammate
    #
    #         player_data = {}
    #         teammate_data = {}
    #
    #         if packet_name == 'PacketParticipantsData' and self.player_car_index == -1:
    #             if self.player_car_index == -1:
    #                 self.player_car_index = packet.header.player_car_index
    #
    #             self.player = packet.participants[self.player_car_index]
    #
    #         # spin until we get setup
    #         if not self.player:
    #             return {}
    #
    #         if packet_name not in ['PacketParticipantsData']:
    #             for name, value in data.items():
    #
    #                 # skip adding header again
    #                 if name == 'header':
    #                     continue
    #
    #                 if isinstance(value, list) and len(value) == 22:
    #                     player_data |= value[self.player_car_index]
    #                     teammate_data |= value[self.teammate_car_index]
    #                 else:
    #                     player_data[name] = value
    #                     teammate_data[name] = value
    #
    #         return {
    #             'type': data_type,
    #             'player_data': player_data,
    #             'teammate_data': teammate_data,
    #         }
    #
    def listen(self):
        session_details = None
        current_lap_number = None
        participants = None
        logger.info(f'Starting server to receive telemetry data on port: {self.port}.')
#
#         session = Session()
#
#         while True:
#             packet, packet_type = self.feed.get_latest()
#
#             if not packet:
#                 continue
#
#             header = packet.header
#
#             key = (header['packet_format'], header['packet_version'],
#                    header['packet_id'])
#
#             packet_name = HEADER_FIELD_TO_PACKET_TYPE[key].__name__
#             if packet_name == 'PacketParticipantsData' and self.player_car_index == -1;
#                 data = self._extract_player(packet=packet)
#
#             if not data:
#                 continue
#
#             # player = data['player_data']
#             # teammate = data['teammate_data']
#             # data_type = data['type']
#             #
#             if data['type'] == 'PacketSessionData' and not session_details:
#                 session_details = Race(
#                     circuit=TRACK_IDS[data['player_data']['track_id']],
#                     session_type=SESSION_TYPE[data['player_data']['session_type']],
#                     session_link_identifier=
#                     data['player_data']['session_link_identifier'],
#                 )
#                 continue
#
#             if data['type'] == 'PacketLapData':
#                 current_lap_number = data['player_data']['current_lap_num']
#
#             if data['type'] == 'PacketCarDamageData':
#                 from datetime import datetime
#                 print(f"{datetime.now().timestamp()},"
#                       f"{data['player_data']['tyre_wear']}")
#
#             if data['type'] == 'PacketParticipantsData' and not participants:
#                 participants = packet.to_dict()['participants']
#
#                 for driver in participants:
#                     driver['name'] = DRIVERS[driver['driver_id']] \
#                         if driver['driver_id'] in DRIVERS else driver['name']
#                     driver['team'] = TEAMS[driver['team_id']] \
#                         if driver['team_id'] in TEAMS else 'Unknown'
#
#             if not current_lap_number or not self.player_car_index > -1 \
#                     or not session_details:
#                 continue
#
#             if self.influxdb:
#                 player_formatted = format_packet(
#                     packet, self.player_car_index, session_details,
#                     current_lap_number, participants)
#                 teammate_formatted = format_packet(
#                     packet, self.player_car_index, session_details, current_lap_number)
#                 if player_formatted:
#                     self.write_to_influxdb(player_formatted)
#                     self.write_to_influxdb(teammate_formatted)
#
#
# def format_packet(packet, player_car_index, session_details,
#                   current_lap_number, participants):
#
#     return formatter(packet, player_car_index, session_details, current_lap_number,
#                      participants)
