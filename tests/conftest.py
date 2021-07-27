import pytest

from honey_ryder.config import InfluxDBConfiguration


@pytest.fixture
def influxdb_config():
    return InfluxDBConfiguration(
        host='127.0.0.1',
        token='tokennn',
        org='org',
        bucket='la_bucket'
    )