import socket
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

    def get_latest(self):

        packet_data = self.listener.get()
        data = packet_data.to_dict()

        # setup car's data location in the array of all cars.
        # setup details about who the player is driving for and their teammate
        if packet_type.__name__ == 'PacketParticipantsData' and self.player_index == -1:
            if not self._player_car_index:
                self.player_index = header.player_car_index

            self.player = data['participants'][self.player_index]

            for i, racer in enumerate(data['participants']):
                if racer['team_id'] == self.player['team_id'] and \
                        racer['driver_id'] != self.player['driver_id']:
                    self.teammate = racer
                    self.teammate_index = i
                    break

        # spin until we get setup
        if not self.player:
            return None, None

        team_data = {
            'header': header.to_dict(),
            'type': packet_type.__name__,
            'player': self.player,
            'teammate': self.teammate,
            'player_data': {},
            'teammate_data': {}
        }

        if packet_type.__name__ not in ['PacketParticipantsData']:
            for name, value in data.items():

                # skip adding header again
                if name == 'header':
                    continue

                if isinstance(value, list) and len(value) == 22:
                    team_data['player_data'] |= value[self.player_index]
                    team_data['teammate_data'] |= value[self.teammate_index]
                else:
                    team_data['player_data'][name] = value
                    team_data['teammate_data'][name] = value

        return data, team_data


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
