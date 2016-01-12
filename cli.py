import click
import importlib

@click.command()
@click.argument('job_path')
def run_job(job_path):
    path, pipeline = job_path.split(':')
    pipeline_module = importlib.import_module(path)

    pipeline = getattr(pipeline_module, pipeline)
    pipeline.run()

if __name__ == '__main__':
    run_job()
