"""
More info is available here:
https://forums.codemasters.com/topic/54423-f1%C2%AE-2020-udp-specification/
"""
#
PACKET_MAPPER = {
    'PacketCarTelemetryData': 'telemetry',
    'PacketLapData': 'lap',
    'PacketMotionData': 'motion',
    'PacketSessionData': 'session',
    'PacketCarStatusData': 'status',
    'PacketCarSetupData': 'setup',
    'PacketParticipantsData': 'participants',
}

TYRE_COMPOUND = {
    16: 'C5',  # super soft
    17: 'C4',
    18: 'C3',
    19: 'C2',
    20: 'C1',  # hard
    7: 'intermediates',
    8: 'wet',
    # F1 Classic
    9: 'dry',
    10: 'wet',
    # F2
    11: 'super soft',
    12: 'soft',
    13: 'medium',
    14: 'hard',
    15: 'wet',
}

WEATHER = {
    0: 'clear',
    1: 'light_cloud',
    2: 'overcast',
    3: 'light_rain',
    4: 'heavy_rain',
    5: 'storm',
}

DRIVER_STATUS = {
    0: 'in_garage',
    1: 'flying_lap',
    2: 'in_lap',
    3: 'out_lap',
    4: 'on_track',
}

SESSION_TYPE = {
    0: 'unknown',
    1: 'practice_1',
    2: 'practice_2',
    3: 'practice_3',
    4: 'short_practice',
    5: 'qualifying_1',
    6: 'qualifying_2',
    7: 'qualifying_3',
    8: 'short_qualifying',
    9: 'osq',
    10: 'race',
    11: 'race_2',
    12: 'time_trial',
}

TRACK_IDS = {
    0: 'Melbourne',
    1: 'Paul Ricard',
    2: 'Shanghai',
    3: 'Sakhir (Bahrain)',
    4: 'Catalunya',
    5: 'Monaco',
    6: 'Montreal',
    7: 'Silverstone',
    8: 'Hockenheim',
    9: 'Hungaroring',
    10: 'Spa',
    11: 'Monza',
    12: 'Singapore',
    13: 'Suzuka',
    14: 'Abu Dhabi',
    15: 'Texas',
    16: 'Brazil',
    17: 'Austria',
    18: 'Sochi',
    19: 'Mexico',
    20: 'Baku (Azerbaijan)',
    21: 'Sakhir Short',
    22: 'Silverstone Short',
    23: 'Texas Short',
    24: 'Suzuka Short',
    25: 'Hanoi',
    26: 'Zandvoort',
    27: 'Imola',
    28: 'Portimão',
    29: 'Jeddah'
}
