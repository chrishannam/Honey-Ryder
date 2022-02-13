import json

import pytest
from pathlib import Path
from honey_ryder.config import InfluxDBConfiguration, RecorderConfiguration
from honey_ryder.recorder import DataRecorder
from honey_ryder.session.session import Race

PACKET_DATA_ROOT = Path(__file__).parent / 'example_packets'


class DummyPacket:
    def __init__(self, data):
        self.data = data

    def to_dict(self):
        return self.data


@pytest.fixture
def race():
    return Race(circuit='test', session_link_identifier=123,
                session_type='practice_test')


@pytest.fixture
def participants():
    with open(PACKET_DATA_ROOT / 'participants.json') as file:
        data = json.load(file)

    return DummyPacket(data)


@pytest.fixture
def telemetry_packet_json():
    with open(PACKET_DATA_ROOT / 'car_telemetry.json') as file:
        data = json.load(file)

    return DummyPacket(data)


@pytest.fixture
def session_packet_json():
    with open(PACKET_DATA_ROOT / 'session.json') as file:
        data = json.load(file)

    return data


@pytest.fixture
def data_recorder():
    return DataRecorder(RecorderConfiguration())


@pytest.fixture
def influxdb_config():
    return InfluxDBConfiguration(
        host='127.0.0.1',
        token='tokennn',
        org='org',
        bucket='la_bucket'
    )