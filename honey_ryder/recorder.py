import logging
from typing import List, Union, Dict

from influxdb_client import Point
from telemetry_f1_2021.cleaned_packets import PacketLapData

from honey_ryder.config import RecorderConfiguration
# from honey_ryder.connectors.heart_beat_monitor import SerialSensor, _detect_port
from honey_ryder.connectors.influxdb.influxdb_connection import InfluxDBConnector
from honey_ryder.connectors.influxdb.influxdb_processor import InfluxDBProcessor
from honey_ryder.connectors.kafka.kafka_connection import KafkaConnector
from honey_ryder.constants import TAGS, BYPASS_PACKETS
from honey_ryder.session.session import Session, Drivers, Driver, CurrentLaps, Lap
from honey_ryder.telemetry.constants import SESSION_TYPE, TRACK_IDS, DRIVERS, TEAMS
from honey_ryder.telemetry.listener import TelemetryFeed


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

    session: Union[Session, None] = None
    drivers: Union[Drivers, None] = None
    laps: Union[CurrentLaps, None] = None
    influxdb_processor: Union[InfluxDBProcessor, None] = None

    def __init__(self, configuration: RecorderConfiguration, port: int = 20777) -> None:
        self.configuration: RecorderConfiguration = configuration
        self.feed = TelemetryFeed(port=port)
        self.port = port
        self.participants = None
        self.tags = TAGS

    @property
    def kafka(self):
        if not self._kafka and self.configuration.kafka and not self._kafka_unavailable:
            self._kafka = KafkaConnector(configuration=self.configuration.kafka)
            if self._kafka.in_error:
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

    def write_to_kafka(self, topic: str, data: List) -> bool:
        if not self.kafka:
            return False

        self.kafka.send(topic, data)
        return True

    # def extract_session_data_influxdb(self, packet: Dict) -> List[Point]:
    #     """
    #     Not all the session data packet is needed, just extract the important stuff.
    #     INFO_DATA_STRING = '{packet_type},' \
    #                'circuit={circuit},lap={lap},' \
    #                'session_uid={session_link_identifier},' \
    #                'session_type={session_type},' \
    #                'team={team},' \
    #                'driver={driver}' \
    #                ' {metric_name}={metric_value}'
    #     """
    #     points = []
    #     for key, value in packet.items():
    #
    #         if key == 'marshal_zones':
    #             for counter, zone in enumerate(value):
    #                 points.append(self.create_point('Session', counter,
    #                                                 zone['zone_flag']))
    #
    #         elif key == 'weather_forecast_samples':
    #             for reading in value:
    #                 for k, v in reading.items():
    #                     points.append(self.create_point('Session', k, v))
    #
    #         else:
    #             points.append(self.create_point('Session', key, value))
    #
    #     return points
    #
    # def create_point(self, packet_name, key, value, driver_name=None, team=None,
    #                  lap=None):
    #     point = Point(packet_name).tag('circuit', self.tags['circuit']) \
    #         .tag('session_uid', self.tags['session_uid']) \
    #         .tag('session_type', self.tags['session_type']) \
    #         .field(key, value)
    #
    #     if team:
    #         point.tag('team', team)
    #
    #     if lap:
    #         point.tag('lap', lap)
    #
    #     if driver_name:
    #         point.tag('driver', driver_name)
    #
    #     return point

    def prepare_process(self, packet, packet_name) -> bool:

        header = packet.header

        packet_dict = packet.to_dict()

        if not self.session and packet_name == 'PacketSessionData':
            self.session = self.process_session(packet_dict, header.session_uid)
            return False

        if not self.drivers and packet_name == 'PacketParticipantsData':
            self.drivers = self.process_drivers(packet_dict)
            return False

        if not self.laps and packet_name == 'PacketLapData':
            self.laps = self.process_laps(packet_dict)
            return False

        return True

    def process_laps(self, data: Dict) -> CurrentLaps:
        laps = []

        for lap in data['lap_data']:
            lap = Lap(**lap)
            laps.append(lap)

        return CurrentLaps(laps=laps)

    def process_session(self, data: Dict, session_link_identifier: int) -> Session:
        circuit = TRACK_IDS[data['track_id']]
        session_type = SESSION_TYPE[data['session_type']]
        return Session(
            circuit=circuit,
            session_type=session_type,
            session_link_identifier=session_link_identifier
        )

    def process_drivers(self, data: Dict) -> Drivers:
        drivers: List[Driver] = []

        for raw_driver in data['participants']:
            if raw_driver['team_id'] != 255:

                # handle custom driver
                if raw_driver['driver_id'] in DRIVERS:
                    driver_name = DRIVERS[raw_driver['driver_id']]
                else:
                    driver_name = raw_driver['name']

                if raw_driver['team_id'] in TEAMS:
                    team = TEAMS[raw_driver['team_id']]
                else:
                    team = 'unknown'

                driver = Driver(
                    ai_controlled=raw_driver['ai_controlled'],
                    driver_name=driver_name,
                    network_id=raw_driver['network_id'],
                    team_name=team,
                    my_team=raw_driver['my_team'],
                    race_number=raw_driver['race_number'],
                    nationality=raw_driver['nationality'],
                    name=raw_driver['name'],
                    your_telemetry=raw_driver['your_telemetry']
                )
                drivers.append(driver)

        return Drivers(drivers=drivers, num_active_cars=data['num_active_cars'])

    def collect(self):

        while True:
            packet, packet_type = self.feed.get_latest()
            packet_name = packet_type.__name__

            if packet_type.__name__ in BYPASS_PACKETS:
                continue

            self.prepare_process(packet, packet_name)

            if not self.session or not self.drivers or not self.laps:
                continue

            if packet_name == 'PacketLapData':
                self.process_laps(packet.to_dict())

            if self.influxdb:
                if not self.influxdb_processor:
                    self.influxdb_processor = InfluxDBProcessor(
                        drivers=self.drivers,
                        session=self.session,
                        laps=self.laps,
                    )
                converted = self.influxdb_processor.convert(packet.to_dict(), packet_name)
                self.write_to_influxdb(converted)


            # packet, packet_type = self.feed.get_latest()
            #
            # # packets to avoid for now
            # if packet_type.__name__ in BYPASS_PACKETS:
            #     continue
            #
            # # packet needs looping and writing to influxdb
            # points = []
            #
            # header = packet.header
            # packet_name = packet_type.__name__
            # packet_dict = packet.to_dict()
            #
            # # updates laps
            # influxdb_points = []
            #
            # del packet_dict['header']
            #
            # if not tags_filled:
            #     if packet_name == 'PacketSessionData':
            #         self.tags['circuit'] = TRACK_IDS[packet.track_id]
            #         self.tags['session_uid'] = header.session_uid
            #         self.tags['session_type'] = SESSION_TYPE[packet_dict['session_type']]
            #         tags_filled = True
            #     else:
            #         continue
            #
            # if packet_name == 'PacketSessionData':
            #     if self.influxdb:
            #         influxdb_points = self.extract_session_data_influxdb(packet_dict)
            #
            # if not self.participants:
            #     if packet_name != 'PacketParticipantsData':
            #         continue
            #     # map drivers to positions in the array of 20
            #     self.participants = packet_dict['participants']
            #     continue
            #
            # if packet_name == 'PacketLapData':
            #     self.laps = packet_dict['lap_data']
            #     laps = packet_dict['lap_data']
            #
            #     if self.influxdb:
            #         influxdb_points = extract_laps_data(packet_dict, self.participants, self.tags)
            #
            # elif packet_name in ['PacketCarSetupData', 'PacketMotionData',
            #                      'PacketCarDamageData', 'PacketCarTelemetryData',
            #                      'PacketCarStatusData']:
            #     if self.influxdb:
            #         influxdb_points = self.extract_car_array_data(packet_dict, packet_name, laps)
            #
            # if self.influxdb and influxdb_points:
            #     self.write_to_influxdb(points)
            # if self.kafka:
            #     topic = packet_name.replace('Packet', '')
            #     topic = topic.replace('Data', '')
            #     topic = topic.lower()
            #     self.write_to_kafka(topic=topic, data=packet_dict)

    def extract_car_array_data(self, packet: Dict, packet_name: str, laps):
        points = []
        packet_name = packet_name.replace('Packet', '').replace('Data', '').replace(
            'Car', '')

        for idx, setup in enumerate(packet[list(packet.keys())[0]]):
            driver = self.participants[idx]

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
                        point = self.create_point(
                            packet_name,
                            key,
                            float(value[location])
                        )

                        if corner.startswith('r'):
                            point.tag('area_of_car', 'rear')
                        else:
                            point.tag('area_of_car', 'front')
                        point.tag('corner_of_car', corner)
                        point.tag('driver', driver_name)
                        point.tag('team', driver_name)
                        points.append(TEAMS[driver['team_id']])
                else:
                    if 'world_forward_dir_' in key or 'world_right_dir_' in key:
                        value = float(value)

                    point = self.create_point(packet_name, key, float(value),
                                              team=TEAMS[driver['team_id']],
                                              driver_name=driver_name,
                                              lap=laps[idx]
                                              )

                    points.append(point)

        return points


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
                try:
                    points.append(Point('LapData').tag('circuit', tags['circuit'])
                              .tag('session_uid', tags['session_uid'])
                              .tag('session_type', tags['session_type'])
                              .tag('team', TEAMS[driver['team_id']])
                              .tag('lap', lap['current_lap_num'])
                              .tag('driver', driver_name)
                              .field(key, float(value))
                    )
                except KeyError as exc:
                    logger.error(f'Missing key "{exc}", while getting lap data.')

    return points


