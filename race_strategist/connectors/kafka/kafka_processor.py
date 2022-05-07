from race_strategist.packet_processing.processor import Processor
from typing import Dict


class KafkaProcessor(Processor):
    def __init__(self):
        pass

    def convert_car_damage_data(self, packet: Dict):
        pass

    def convert_car_telemetry_data(self, packet: Dict):
        pass

    def convert_car_status_data(self, packet: Dict):
        pass

    def convert_motion_data(self, packet: Dict):
        pass

    def convert_car_setup_data(self, packet: Dict):
        return packet

    def save(self, data):
        pass
