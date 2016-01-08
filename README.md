[![Build Status](https://travis-ci.org/UCSUR-Pitt/wprdc-etl.svg?branch=master)](https://travis-ci.org/UCSUR-Pitt/wprdc-etl)

# WPRDC-ETL

ETL Processes for the Western Pennsylvania Regional DataCenter (www.wprdc.org).

### Develop

It is highly recommended that you use use [virtualenv](https://readthedocs.org/projects/virtualenv/) (and [virtualenvwrapper](https://virtualenvwrapper.readthedocs.org/en/latest/) for convenience). For a how-to on getting set up, please consult this [howto](https://github.com/codeforamerica/howto/blob/master/Python-Virtualenv.md).

Once you have a virtualenv, activate it. From there, the following steps can be used to bootstrap your environment:

```bash
git clone https://github.com/UCSUR-Pitt/wprdc-etl
cd wprdc-etl
# create a copy of the settings
cp settings.json.example settings.json
# after the settings have been copied, you will need to go in and
# edit the various values for your setup. at this point, you should
# be ready to create the status database
python create_db.py --server NAME_OF_YOUR_SERVER_HERE
# note, if you need to destroy and recreate the status data at any point
# you can do so with python create_db.py --server NAME_OF_YOUR_SERVER_HERE --drop
```