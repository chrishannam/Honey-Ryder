from telemetry_f1_2021.packets import PacketCarTelemetryData

from honey_ryder.session.session import Race


def formatter(data: PacketCarTelemetryData, index: int, race: Race, lap: int):
    """git
    """

    formatted_data = []

    for key, value in data.to_dict().items():
        if key == 'm_header':
            continue
        elif key == 'm_car_telemetry_data':
            player = value[index]
            for sub_key, sub_value in player.items():
                if isinstance(sub_value, list) and len(sub_value) == 4:
                    for i, corner in enumerate(['rl', 'rr', 'fl', 'fr']):
                        end = 'front' if corner.startswith('f') else 'rear'
                        formatted_data.append(
                            f'telemetry,track={race.circuit},lap={lap},'
                            f'session_uid={race.session_link_identifier},'
                            f'session_type={race.session_type},corner={corner},'
                            f'area_of_car={end}'
                            f" {sub_key.replace('m_' , '', 1)}={sub_value[i]}"
                        )
                else:
                    formatted_data.append(
                        f'motion,track={race.circuit},lap={lap},'
                        f'session_uid={race.session_link_identifier},'
                        f'session_type={race.session_type}'
                        f" {sub_key.replace('m_' , '', 1)}={sub_value}"
                    )
        else:
            formatted_data.append(
                f'motion,track={race.circuit},lap={lap},'
                f'session_uid={race.session_link_identifier},'
                f'session_type={race.session_type} {key}={value}'
            )
    return formatted_data
