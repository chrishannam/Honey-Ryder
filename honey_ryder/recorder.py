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
from honey_ryder.packet_processing.processor import process_laps, process_session_history, process_drivers, \
    process_session
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
    session_history = {}

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

    def prepare_for_processing(self, packet, packet_name) -> bool:

        header = packet.header

        packet_dict = packet.to_dict()

        if not self.session and packet_name == 'PacketSessionData':
            self.session = process_session(packet_dict, header.session_uid)
            return False
        elif packet_name == 'PacketSessionData':
            if self.session.session_link_identifier != header.session_uid:
                self.session = None
                self.drivers = None
                self.laps = None

        if not self.drivers and packet_name == 'PacketParticipantsData':
            self.drivers = process_drivers(packet_dict)
            return False

        if not self.laps and packet_name == 'PacketLapData':
            self.laps = process_laps(packet_dict)
            return False

        if packet_name == 'PacketLapData':
            self.session_history = process_session_history(packet_dict)

        return True

    def collect(self):

        while True:
            packet, packet_type = self.feed.get_latest()
            packet_name = packet_type.__name__

            if packet_type.__name__ in BYPASS_PACKETS:
                continue

            self.prepare_for_processing(packet, packet_name)

            if not self.session or not self.drivers or not self.laps:
                continue

            if packet_name == 'PacketLapData':
                self.laps = process_laps(packet.to_dict())

            if self.influxdb:
                if not self.influxdb_processor:
                    self.influxdb_processor = InfluxDBProcessor(
                        drivers=self.drivers,
                        session=self.session,
                        laps=self.laps,
                    )
                self.influxdb_processor.update_laps(self.laps)
                converted = self.influxdb_processor.convert(packet.to_dict(), packet_name)

                if converted:
                    self.write_to_influxdb(converted)
