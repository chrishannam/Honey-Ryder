from datetime import datetime

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# You can generate an API token from the "API Tokens Tab" in the UI
from telemetry.constants import TYRE_COMPOUND

token = "nRmeV1gIRAstJJVlDW6pWdf0qtsQ1hgsk7qLMl_2nZfNeWKbBx8TPE_5KfxCsAW44beNyWWh8RdpgKzuU4VF3g=="
org = "f1"
bucket = "f1"


def lap_times():
    with InfluxDBClient(url="http://ultron:8086", token=token, org=org) as client:
        query = """
        from(bucket: "f1")
  |> range(start: -30m)
  |> filter(fn: (r) => r["_measurement"] == "SessionHistory")
  |> filter(fn: (r) => r["_field"] == "lap_time_in_ms")
  |> filter(fn: (r) => r["lap"] == "5")
  |> aggregateWindow(every: 500ms, fn: mean, createEmpty: false)
  |> yield(name: "mean")
  """
        #   |> filter(fn: (r) => r["driver_name"] == "Charles Leclerc")
        results = {}
        tables = client.query_api().query(query, org=org)
        for table in tables:
            for record in table.records:
                driver_name = record.values.get('driver_name')
                if driver_name not in results:
                    results[driver_name] = {}

                results[driver_name]['lap'] = record.values.get('lap')
                results[driver_name]['last_lap'] = convert_lap_time(record.get_value())
        return results


def convert_lap_time(lap_time: int) -> str:
    seconds, milliseconds = divmod(lap_time, 1000)
    minutes, seconds = divmod(seconds, 60)
    return f'{int(minutes):02d}:{int(seconds):02d}.{int(milliseconds):03d}'


def all_lap_times():
    with InfluxDBClient(url="http://ultron:8086", token=token, org=org) as client:
        query = """
        from(bucket: "f1")
          |> range(start: -100m)
          |> filter(fn: (r) => r["_measurement"] == "SessionHistory")
          |> filter(fn: (r) => r["_field"] == "lap_time_in_ms" or r["_field"] == "tyre_actual_compound")
          |> filter(fn: (r) => r["lap"] == "5")
          |> aggregateWindow(every: 500ms, fn: mean, createEmpty: false)
          |> yield(name: "mean")
          """
        #   |> filter(fn: (r) => r["driver_name"] == "Charles Leclerc")
        results = {}
        tables = client.query_api().query(query, org=org)
        for table in tables:
            for record in table.records:
                driver_name = record.values.get('driver_name')
                if driver_name not in results:
                    results[driver_name] = {}

                if record.get_field() == 'tyre_actual_compound':
                    results[driver_name]['tyre_actual_compound'] = TYRE_COMPOUND[record.get_value()]
                else:
                    results[driver_name]['lap'] = record.values.get('lap')
                    results[driver_name]['last_lap'] = convert_lap_time(record.get_value())
                    results[driver_name]['name'] = driver_name

        return results


if __name__ == '__main__':
    results = all_lap_times()
    print(results)
