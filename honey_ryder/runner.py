import click

from honey_ryder.config import load_config
from honey_ryder.recorder import DataRecorder


@click.command()
@click.option("--port", default=20777, help="port to listen on")
def run(port: int = 20777):
    config = load_config()
    recorder = DataRecorder(config, port=port)
    recorder.collect()


if __name__ == "__main__":
    run()