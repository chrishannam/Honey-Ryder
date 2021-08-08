from typing import Union, Dict
from telemetry_f1_2021.packets import HEADER_FIELD_TO_PACKET_TYPE, PacketHeader
from telemetry_f1_2021.listener import TelemetryListener


class TelemetryFeed:

    _player_car_index: Union[int, None] = None
    _player_team: str
    teammate: Dict
    teammate_index: int = -1
    player_index: int = -1
    player: Union[Dict, None] = None

    def __init__(self, port: int = None, host: str = None):
        if not port:
            port = 20777

        if not host:
            host = ''
        self.listener = TelemetryListener(port=port, host=host)

    def player_car_index(self, header: PacketHeader = None):
        if not self._player_car_index and header:
            self._player_car_index = header.player_car_index
        return self._player_car_index

    def get_latest(self) -> Dict:
        return self.listener.get()


def _flat(key, data) -> Dict:
    """
    Extract out tyre lists.
    The order is as follows [RL, RR, FL, FR]
    """
    flat_data = {}
    if isinstance(data, dict):
        for k, v in data.items():
            # check for tyre lists
            if isinstance(v, list) and len(v) == 4:
                flat_data[f'{k}_rl'] = v[0]
                flat_data[f'{k}_rr'] = v[1]
                flat_data[f'{k}_fl'] = v[2]
                flat_data[f'{k}_rf'] = v[3]
            if isinstance(v, dict):
                a = 1
            else:
                flat_data[k] = v
    elif isinstance(data, list):
        flat_data[f'{key}_rl'] = data[0]
        flat_data[f'{key}_rr'] = data[1]
        flat_data[f'{key}_fl'] = data[2]
        flat_data[f'{key}_rf'] = data[3]
    return flat_data
