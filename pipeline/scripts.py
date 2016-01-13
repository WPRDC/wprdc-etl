import os
import sqlite3
import click
import json
import importlib

HERE = os.path.abspath(os.path.dirname(__file__))

@click.command()
@click.argument('config', type=click.Path(exists=True))
@click.option('--server', type=click.STRING)
@click.option('--drop', type=click.BOOL, is_flag=True)
def create_db(config, server, drop):
    with open(config) as f:
        try:
            settings_file = json.loads(f.read())
            settings = settings_file[server]
        except KeyError:
            raise click.BadParameter(
                'invalid choice: {}. (choose from {})'.format(
                    server, ', '.join(settings_file.keys())
                )
            )
        except json.decoder.JSONDecodeError:
            raise click.ClickException(
                'invalid JSON in settings file'
            )

    conn = sqlite3.connect(settings['statusdb'])
    cur = conn.cursor()

    if drop:
        click.echo('Dropping table...')
        cur.execute('''DROP TABLE IF EXISTS status''')
        conn.commit()

    click.echo('Creating table...')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS
    status (
        name TEXT NOT NULL,
        display_name TEXT,
        last_ran INTEGER,
        start_time INTEGER NOT NULL,
        status TEXT,
        num_lines INTEGER,
        PRIMARY KEY (display_name, start_time)
    )
    ''')
    conn.commit()

@click.command()
@click.argument('job_path', type=click.STRING)
def run_job(job_path):
    try:
        path, pipeline = job_path.split(':')
        pipeline_module = importlib.import_module(path)
        pipeline = getattr(pipeline_module, pipeline)
    except:
        raise click.ClickException(
            'A Pipeline could not be found at "{}"'.format(
                job_path
            )
        )

    pipeline.run()
