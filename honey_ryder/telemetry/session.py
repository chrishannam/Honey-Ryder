from typing import NamedTuple


class Conditions(NamedTuple):
    track_temperature: int
    air_temperature: int


class Session:
    """
    Represents what is happening on track for this running process.
    """
    def __init__(self, packet):
        self.session_uid = packet.header.session_uid
        self.player_car_index = packet.header.player_car_index
        self.team_mate_car_index = None
        self.participants = None


def convert_packet(packet):
    pass


def _extract_session_data(data):
    return Conditions(
        track_temperature=data['track_temperature'],
        air_temperature=data['air_temperature'],
    )
