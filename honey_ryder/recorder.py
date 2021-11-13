import logging
from typing import List, Union, Dict

from influxdb_client import Point
from telemetry_f1_2021.cleaned_packets import HEADER_FIELD_TO_PACKET_TYPE

from honey_ryder.config import RecorderConfiguration
# from honey_ryder.connectors.heart_beat_monitor import SerialSensor, _detect_port
from honey_ryder.connectors.influxdb.influxdb import InfluxDBConnector
from honey_ryder.connectors.kafka import KafkaConnector
from honey_ryder.constants import TAGS, BYPASS_PACKETS
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
        self.tags = TAGS

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

    def extract_session_data(self, packet: Dict) -> List[Point]:
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
        points = []
        for key, value in packet.items():

            if key == 'weather':
                points.append(self.create_point(key, WEATHER[value]))

            elif key == 'marshal_zones':
                for counter, zone in enumerate(value):
                    points.append(self.create_point(counter, zone['zone_flag']))

            elif key == 'weather_forecast_samples':
                for reading in value:
                    for k, v in reading.items():
                        points.append(self.create_point(k, v))

            else:
                points.append(self.create_point(key, value))

        return points

    def create_point(self, key, value):
        return Point("SessionData").tag('circuit', self.tags['circuit']) \
                        .tag('session_uid', self.tags['session_uid']) \
                        .field(key, value)

    def collect(self):
        tags_filled = False

        laps = False
        while True:
            packet, packet_type = self.feed.get_latest()

            # packets to avoid for now
            if packet_type.__name__ in BYPASS_PACKETS:
                continue

            driver = packet.header.player_car_index

            # packet needs looping and writing to influxdb
            points = []

            header = packet.header
            packet_name = packet_type.__name__
            packet_dict = packet.to_dict()

            del packet_dict['header']

            if not tags_filled:
                if packet_name == 'PacketSessionData':
                    self.tags['circuit'] = TRACK_IDS[packet.track_id]
                    self.tags['session_uid'] = header.session_uid
                    self.tags['session_type'] = SESSION_TYPE[packet_dict['session_type']]
                    tags_filled = True
                else:
                    continue

            if packet_name == 'PacketSessionData':
                points = self.extract_session_data(packet_dict)

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
                points = extract_laps_data(packet_dict, self.participants, self.tags)

            elif packet_name in ['PacketCarSetupData', 'PacketMotionData',
                                 'PacketCarDamageData', 'PacketCarTelemetryData',
                                 'PacketCarStatusData']:
                points = extract_car_data(packet_dict, self.participants, TAGS, laps)

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


