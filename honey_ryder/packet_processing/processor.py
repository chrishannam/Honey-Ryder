from abc import ABC, abstractmethod

from honey_ryder.session.session import Drivers, Driver
from honey_ryder.telemetry.constants import TEAMS, DRIVERS


class Processor(ABC):

    @abstractmethod
    def convert(self, data):
        pass


def process_participants(participants_packet_data):
    drivers = []
    for raw_driver in participants_packet_data['participants']:
        if raw_driver['team_id'] != 255:
            # handle custom driver
            if raw_driver['driver_id'] in DRIVERS:
                driver_name = DRIVERS[raw_driver['driver_id']]
            else:
                driver_name = raw_driver['name']

            if raw_driver['team_id'] in TEAMS:
                team_name = TEAMS[raw_driver['team_id']]
            else:
                team_name = 'Unknown'

            driver = Driver(
                ai_controlled=raw_driver['ai_controlled'],
                driver_name=driver_name,
                network_id=raw_driver['network_id'],
                team_name=team_name,
                my_team=raw_driver['my_team'],
                race_number=raw_driver['race_number'],
                nationality=raw_driver['nationality'],
                name=raw_driver['name'],
                your_telemetry=raw_driver['your_telemetry']
            )
            drivers.append(driver)

    return Drivers(drivers=drivers, num_active_cars=participants_packet_data['num_active_cars'])
