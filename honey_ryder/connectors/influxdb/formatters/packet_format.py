from typing import Dict, List, Union, Any

from telemetry_f1_2021.packets import Packet, \
    HEADER_FIELD_TO_PACKET_TYPE, PacketEventData, PacketSessionData, \
    PacketParticipantsData

from honey_ryder.session.session import Race

INFO_DATA_STRING = '{packet_type},' \
                   'circuit={circuit},lap={lap},' \
                   'session_uid={session_link_identifier},' \
                   'session_type={session_type},' \
                   'team={team},' \
                   'driver={driver}' \
                   ' {metric_name}={metric_value}'

CAR_DATA_STRING = '{packet_type},' \
                  'circuit={circuit},lap={lap},' \
                  'session_uid={session_link_identifier},' \
                  'session_type={session_type},' \
                  'team={team},' \
                  'driver={driver},' \
                  'area_of_car={end},' \
                  'corner={corner}' \
                  ' {metric_name}={metric_value}'


def formatter(data, index: int, race: Race, lap: int, participants) -> \
        Union[Union[dict, list[str]], Any]:
    """
    index is the position in list of the car we are interested in.
    """
    formatted = {}

    if data.__class__.__name__ in ['PacketParticipantsData',
                                   'PacketSessionHistoryData']:
        return {}
        # return _format_participants_packet(data, index, race, lap)

    elif data.__class__.__name__ == 'PacketSessionData':
        return format_session_packet(data, index, race, lap)

    elif data.__class__.__name__ == 'PacketEventData':
        return format_event_packet(data, index, race, lap)

    else:
        return format_grid_packet(data, index, race, lap, participants)


def format_event_packet(data: PacketEventData, index: int, race: Race, lap: int,
                        log_player_only=True):
    """
        Grid packet has all 22 cars available data.
    """

    formatted_data = []
    packet_type = None

    for key, value in data.to_dict().items():
        if key == 'm_header':
            header = value
            key = (header['m_packet_format'],
                   header['m_packet_version'], header['m_packet_id'])
            packet_type = HEADER_FIELD_TO_PACKET_TYPE[key].__name__
        elif key == 'm_fastest_lap':
            formatted_data.append(
                f'{packet_type.replace("Packet", "")},'
                f'circuit={race.circuit},lap={lap},'
                f'session_uid={race.session_link_identifier},'
                f'session_type={race.session_type} fastest_lap='
                f'{value["m_fastest_lap"]["m_lap_time"]}'
            )
        else:
            continue
    return formatted_data


def format_session_packet(data: PacketSessionData, player_index: int, race: Race,
                          lap: int):
    """
        Grid packet has all 22 cars available data.
    """

    formatted_data = []
    packet_type = None

    for key, value in data.to_dict().items():
        if key == 'm_header':
            header = value
            key = (header['m_packet_format'],
                   header['m_packet_version'], header['m_packet_id'])
            packet_type = HEADER_FIELD_TO_PACKET_TYPE[key].__name__
        elif key in ['m_weather_forecast_samples', 'm_marshal_zones']:
            continue
        else:
            formatted_data.append(
                f'{packet_type.replace("Packet", "")},'
                f'circuit={race.circuit},lap={lap},'
                f'session_uid={race.session_link_identifier},'
                f'session_type={race.session_type} {key.replace("m_", "", 1)}={value}'
            )
    return formatted_data


def format_grid_packet(data: Packet, index: int, race: Race, lap: int,
                       participants: List, log_player_only=False):
    """
        Grid packet has all 22 cars available data.
    """

    formatted_data = []
    packet_type: str = 'unknown'
    if participants[index]['name'] not in ['', 'unknown']:
        driver_name = participants[index]['name'].replace(' ', '\ ')
        driver_team = participants[index]['team'].replace(' ', '\ ')
    else:
        return None

    for key, value in data.to_dict().items():
        if key == 'm_header':
            header = value
            key = (header['m_packet_format'],
                   header['m_packet_version'], header['m_packet_id'])
            packet_type = HEADER_FIELD_TO_PACKET_TYPE[key].__name__

        elif isinstance(value, list) and len(value) == 22:
            if log_player_only:
                formatted_data += log_driver_from_list(value[index], race, packet_type)
            else:
                for driver_index, driver_data in enumerate(value):
                    driver_details = participants[driver_index]
                    if driver_details['m_race_number'] != 0:
                        formatted_data += log_driver_from_list(driver_details, race,
                                                                   packet_type,
                                                               lap, driver_data)

        elif isinstance(value, list) and len(value) == 4:
            # each corner of the car in this order RL, RR, FL, FR
            for i, corner in enumerate(['rl', 'rr', 'fl', 'fr']):
                end = 'front' if corner.startswith('f') else 'rear'
                formatted_data.append(
                    f'{packet_type.replace("Packet", "")},'
                    f'circuit={race.circuit},lap={lap},'
                    f'session_uid={race.session_link_identifier},'
                    f'session_type={race.session_type},corner={corner},'
                    f'team={driver_team},'
                    f'driver={driver_name},'
                    f'area_of_car={end}'
                    f' {key.replace("m_", "", 1)}={value[i]}'
                )
        else:
            formatted_data.append(
                f'{packet_type.replace("Packet", "")},'
                f'circuit={race.circuit},lap={lap},'
                f'session_uid={race.session_link_identifier},'
                f'team={driver_team},'
                f'driver={driver_name},'
                f'session_type={race.session_type} {key.replace("m_", "", 1)}={value}'
            )
    return formatted_data


def log_driver_from_list(driver,
                         race, packet_type,
                         lap, data) -> List:
    formatted_data = []

    for sub_key, sub_value in data.items():
        if isinstance(sub_value, list) and len(sub_value) == 4:
            # each corner of the car in this order RL, RR, FL, FR
            for i, corner in enumerate(['rl', 'rr', 'fl', 'fr']):
                end = 'front' if corner.startswith('f') else 'rear'
                # formatted_data.append(
                #     f'{packet_type.replace("Packet", "")},'
                #     f'circuit={race.circuit},lap={lap},'
                #     f'session_uid={race.session_link_identifier},'
                #     f'session_type={race.session_type},corner={corner},'
                #     f'area_of_car={end}'
                #     f' {sub_key}={sub_value[i]}'
                # )
                formatted_data.append(
                        CAR_DATA_STRING.format(
                            packet_type=packet_type.replace("Packet", ""),
                            circuit=race.circuit,
                            lap=lap,
                            team=driver['team'].replace(' ', '\ '),
                            driver=driver['name'].replace(' ', '\ '),
                            session_link_identifier=race.session_link_identifier,
                            session_type=race.session_type,
                            end=end,
                            corner=corner,
                            metric_name=sub_key.replace('m_', '', 1),
                            metric_value=sub_value[i],
                        )
                )
        else:

            formatted_data.append(
                INFO_DATA_STRING.format(
                    packet_type=packet_type.replace("Packet", ""),
                    circuit=race.circuit,
                    lap=lap,
                    team=driver['team'].replace(' ', '\ '),
                    driver=driver['name'].replace(' ', '\ '),
                    session_link_identifier=race.session_link_identifier,
                    session_type=race.session_type,
                    metric_name=sub_key.replace('m_', '', 1),
                    metric_value=sub_value,
                )
            )

    return formatted_data


def format_participants_packet(data: PacketParticipantsData, index: int, race: Race,
                               lap: int):
    """
    """

    formatted_data = []

    for key, value in data.to_dict().items():
        if key == 'm_header':
            continue
        elif key.endswith('_participants'):
            player = value[index]
            for top_level_key, top_level_value in player.items():
                formatted_data.append(
                    f'participants,circuit={race.circuit},lap={lap},'
                    f'session_uid={race.session_link_identifier},'
                    f'session_type={race.session_type}'
                    f" {top_level_key.replace('m_', '', 1)}={top_level_value}"
                )
        else:
            formatted_data.append(
                f'participants,circuit={race.circuit},lap={lap},'
                f'session_uid={race.session_link_identifier},'
                f"session_type={race.session_type} {key.replace('m_', '', 1)}={value}"
            )
    return formatted_data
