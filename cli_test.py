import click
import importlib

@click.command()
@click.argument('path')
def show_path(path):
    path, pipeline = path.split(':')
    pipeline_module = importlib.import_module(path)

    pipeline = getattr(pipeline_module, pipeline)
    pipeline.run()

if __name__ == '__main__':
    show_path()
