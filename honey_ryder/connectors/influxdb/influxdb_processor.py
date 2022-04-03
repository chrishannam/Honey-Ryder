from influxdb_client import Point
from typing import Dict, List

from honey_ryder.packet_processing.processor import Processor
from honey_ryder.session.session import Session, Lap, Drivers, Driver, CurrentLaps


class InfluxDBProcessor(Processor):
    def __init__(self, session: Session, drivers: Drivers, laps: CurrentLaps):
        self.session = session
        self.drivers = drivers
        self.laps: CurrentLaps = laps

    def convert(self, data: Dict, packet_name: str):

        if packet_name in ['PacketCarSetupData', 'PacketMotionData',
                           'PacketCarDamageData', 'PacketCarTelemetryData',
                           'PacketCarStatusData']:
            return self.extract_car_array_data(packet=data, packet_name=packet_name)

    def update_laps(self, laps: CurrentLaps):
        self.laps = laps

    def create_point(self, packet_name: str, key: str, value: float, lap: int, driver: Driver, team: str,
                     tags: Dict = None) -> Point:

        if tags is None:
            tags = dict()

        point = Point(packet_name).tag('circuit', self.session.circuit) \
            .tag('session_uid', self.session.session_link_identifier) \
            .tag('session_type', self.session.session_type) \
            .tag('driver_name', driver.driver_name) \
            .tag('team', team) \
            .tag('lap', lap) \
            .field(key, value)

        for tag_name, tag_value in tags.items():
            point.tag(tag_name, tag_value)

        return point

    def extract_car_array_data(self, packet: Dict, packet_name: str):
        points = []
        data_name = packet_name.replace('Packet', '').replace('Data', '').replace('Car', '')

        for idx, data in enumerate(packet[list(packet.keys())[1]]):
            if idx >= len(self.drivers.drivers):
                continue

            lap_number = self.laps.laps[idx].current_lap_num

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
        return points
