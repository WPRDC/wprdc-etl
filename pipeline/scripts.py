import os
import sqlite3
import click
import json
import importlib
from pipeline import Pipeline
from pipeline.exceptions import InvalidPipelineError, DuplicateFileException

HERE = os.path.abspath(os.path.dirname(__file__))

@click.command()
@click.option(
    '--config', '-c', type=click.Path(exists=True),
    help='Path to a configuration object to use.')
@click.option(
    '--db', '-D', default='./status.db', type=click.Path(),
    help='Path at which to create sqlite3 database.')
@click.option(
    '--drop', '-d', type=click.BOOL, is_flag=True,
    help='Whether or not to drop and recreate the table.')
def create_db(config, db, drop):
    '''Create a status table based on the passed CONFIG json file or destination path
    '''
    if config:
        with open(config) as f:
            try:
                settings = json.loads(f.read())
                conn = sqlite3.connect(settings['general']['statusdb'])

            except json.decoder.JSONDecodeError:
                raise click.ClickException(
                    'invalid JSON in settings file'
                )
            except KeyError:
                raise click.ClickException(
                    'CONFIG must contain a location for a statusdb'
                )

    else:
        try:
            conn = sqlite3.connect(db)
        except KeyError:
            raise click.ClickException(
                'Must provide a valid path to create statusdb'
            )


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
        input_checksum TEXT,
        status TEXT,
        num_lines INTEGER,
        PRIMARY KEY (display_name, start_time)
    )
    ''')
    conn.commit()

@click.command()
@click.argument('job_path', type=click.STRING)
@click.option(
    '--config', type=click.Path(exists=True),
    help='Path to a configuration object to use')
def run_job(job_path, config):
    '''Run a pipeline based on the given input JOB_PATH

    Directories should be separated based on the . character
    and the pipeline should be separated from the directories
    with a : character.

    For example: my.nested.job.directory:my_pipeline
    '''
    try:
        if ':' not in job_path:
            raise InvalidPipelineError
        path, pipeline = job_path.split(':')
        pipeline_module = importlib.import_module(path)
        pipeline = getattr(pipeline_module, pipeline)
        if not isinstance(pipeline, Pipeline):
            raise InvalidPipelineError

        if config:
            pipeline.set_config_from_file(config)

        pipeline.run()

    except (InvalidPipelineError, ImportError):
        raise click.ClickException(
            'A Pipeline could not be found at "{}"'.format(
                job_path
            )
        )

    except DuplicateFileException:
        raise click.ClickException(
            'This input has already been processed!'
        )

    except Exception as e:
        raise click.ClickException(
            'Something went wrong in the pipeline: {}'.format(e)
        )
