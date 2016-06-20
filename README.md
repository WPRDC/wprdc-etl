[![Build Status](https://travis-ci.org/WPRDC/wprdc-etl.svg?branch=master)](https://travis-ci.org/WPRDC/wprdc-etl)
[![Documentation Status](https://readthedocs.org/projects/wprdc-etl/badge/?version=latest)](https://wprdc-etl.readthedocs.org/en/latest/)

# WPRDC-ETL

ETL Processes for the [Western Pennsylvania Regional DataCenter](https://www.wprdc.org). 

To see examples of how it's used, check out the [WPRDC ETL Jobs](https://github.com/WPRDC/etl-jobs).
### Develop

It is highly recommended that you use use [virtualenv](https://readthedocs.org/projects/virtualenv/) (and [virtualenvwrapper](https://virtualenvwrapper.readthedocs.org/en/latest/) for convenience). For a how-to on getting set up, please consult this [howto](https://github.com/codeforamerica/howto/blob/master/Python-Virtualenv.md).

Once you have a virtualenv, activate it. From there, the following steps can be used to bootstrap your environment:

```bash
git clone https://github.com/UCSUR-Pitt/wprdc-etl
cd wprdc-etl
# install dependencies. if you just need access the Pipeline and related
# classes, you can install via setup.py:
pip install -e .
# if you want to develop, you'll need to install additional requirements
# (such as the test runner):
pip install -r requirements.txt
# At this point, if you'd like to track the status of your etl jobs, you should
# be ready to create the status database. By installing the package above
# you should have access to two command-line commands, one of which
# will create the database for you
create_monitoring_db <path/to/status_database.db>
# note, if you need to destroy and recreate the status data at any point
# you can do so with:
# create_monitoring_db <path/to/status_database.db> --drop
```

### Docs

Documentation is stored in the docs directory. To make and view docs locally, run the following (on a mac):

```bash
cd docs
make html
open _build/html/index.html
```

A make.bat file is included for windows users as well.

