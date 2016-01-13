from setuptools import setup, find_packages

setup(
    name='pipeline',
    version='0.1',
    package=find_packages(),
    include_package_data=True,
    install_requires=[
        'Click>6,<7', 'marshmallow>2.4,<3', 'requests>2.9,<3'
    ],
    entry_points='''
    [console_scripts]
    create_monitoring_db=pipeline.scripts:create_db
    run_job=pipeline.scripts:run_job
    '''
)
