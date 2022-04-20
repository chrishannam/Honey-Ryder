
from flask import Flask, render_template
from influxdb_client import InfluxDBClient

# You can generate an API token from the "API Tokens Tab" in the UI
from honey_ryder.constants import TYRE_COMPOUND

token = "nRmeV1gIRAstJJVlDW6pWdf0qtsQ1hgsk7qLMl_2nZfNeWKbBx8TPE_5KfxCsAW44beNyWWh8RdpgKzuU4VF3g=="
org = "f1"
bucket = "f1"


app = Flask(__name__,
            static_url_path='',
            static_folder='web/static',
            template_folder='web/templates')


def main():
    # return 17
    values = {}
    with InfluxDBClient(url="http://ultron:8086", token=token, org=org) as client:
        query = """
from(bucket: "f1")
  |> range(start: -10m)
  |> filter(fn: (r) => r["_measurement"] == "Session")
  |> filter(fn: (r) => r["_field"] == "pit_stop_window_ideal_lap" 
  or r["_field"] == "pit_stop_window_latest_lap" 
  or r["_field"] == "pit_stop_rejoin_position" 
  or r["_field"] == "air_temperature" 
  or r["_field"] == "air_temperature_change" 
  or r["_field"] == "track_temperature" 
  or r["_field"] == "track_temperature_change")
  |> aggregateWindow(every: 500ms, fn: mean, createEmpty: false)
  |> yield(name: "mean")
  """
        tables = client.query_api().query(query, org=org)
        for table in tables:
            for record in table.records:
                values[record.values['_field']] = int(record.get_value())

    return values

#
# def all_lap_times():
#     with InfluxDBClient(url="http://ultron:8086", token=token, org=org) as client:
#         query = """
#         from(bucket: "f1")
#           |> range(start: -100m)
#           |> filter(fn: (r) => r["_measurement"] == "SessionHistory")
#           |> filter(fn: (r) => r["_field"] == "lap_time_in_ms" or r["_field"] == "tyre_actual_compound")
#           |> filter(fn: (r) => r["lap"] == "5")
#           |> aggregateWindow(every: 500ms, fn: mean, createEmpty: false)
#           |> yield(name: "mean")
#           """
#         #   |> filter(fn: (r) => r["driver_name"] == "Charles Leclerc")
#         results = []
#         tables = client.query_api().query(query, org=org)
#         for table in tables:
#             for record in table.records:
#                 driver_name = record.values.get('driver_name')
#                 driver = {
#                     'lap': record.values.get('lap'),
#                     'last_lap': convert_lap_time(record.get_value()),
#                     'name': driver_name
#                 }
#                 results.append(driver)
#                 break
#         return results

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


def convert_lap_time(lap_time: int) -> str:
    seconds, milliseconds = divmod(lap_time, 1000)
    minutes, seconds = divmod(seconds, 60)
    return f'{int(minutes):02d}:{int(seconds):02d}.{int(milliseconds):03d}'


@app.route("/")
def hello():
    pit_stop_stats = main()
    return render_template('index.html', **pit_stop_stats)


@app.route("/lap_times")
def laps():
    lap_times = all_lap_times()
    print(lap_times)
    for k, v in lap_times.items():
        print(f'{k}: {v["last_lap"]}')
    return render_template('lap_times.html', lap_times=lap_times)


if __name__ == "__main__":
    data = main()
    print(data)
