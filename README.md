[![Build Status](https://travis-ci.org/WPRDC/wprdc-etl.svg?branch=master)](https://travis-ci.org/WPRDC/wprdc-etl)
[![Documentation Status](https://readthedocs.org/projects/wprdc-etl/badge/?version=latest)](https://wprdc-etl.readthedocs.org/en/latest/)

# WPRDC-ETL

ETL Processes for the [Western Pennsylvania Regional DataCenter](https://www.wprdc.org). 

To see examples of how it's used, check out the [WPRDC ETL Jobs](https://github.com/WPRDC/etl-jobs).



### Develop

It is highly recommended that you use use [virtualenv](https://readthedocs.org/projects/virtualenv/) (and [virtualenvwrapper](https://virtualenvwrapper.readthedocs.org/en/latest/) for convenience). For a how-to on getting set up, please consult this [howto](https://github.com/codeforamerica/howto/blob/master/Python-Virtualenv.md).

Once you have a virtualenv, activate it. From there, the following steps can be used to bootstrap your environment:

```bash
git clone https://github.com/WPRDC/wprdc-etl
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
create_monitoring_db <path/to/new_status_database.db>
# note, if you need to destroy and recreate the status data at any point
# you can do so with:
# create_monitoring_db <path/to/status_database.db> --drop
```

### Sample Job
The following commands will create a database in which to store ETL job statuses and insert data from example.csv to a CKAN instance running at http://data.wprdc.org.

```bash
cd /path/to/jobs_dir/
# create a status tracking sqlite db called status.db in jobs_dir/
create_monitoring_db status.db
# note, if you need to destroy and recreate the status data in status.db
# at any point, you can do so with:
# create_monitoring_db status.db --drop
# run example_pipeline from example_job.py in jobs_dir/
run_job example_job:example_pipeline
```

##### example.csv:
| name      | birthdate  | last_visit       | visit_count |
|-----------|------------|------------------|-------------|
| Susan     | 03/09/2016 | 01/25/2016T03:07 | 21          |
| Xenos     | 10/29/2015 | 01/01/2016T22:41 | 9           |
| Jordan    | 04/27/2016 | 10/25/2015T22:14 | 50          |
| Xander    | 03/21/2017 | 06/19/2016T12:59 | 40          |

##### example_job.py:
```python
import os
import datetime
from marshmallow import fields, pre_load
import pipeline as pl

class ExampleSchema(pl.BaseSchema):
    name = fields.String()
    birthdate = fields.Date(format='%m/%d/%Y')
    last_visit = fields.DateTime(format='%m/%d/%YT%H:%M')
    visit_count = fields.Integer()

    class Meta:
        ordered=True

    @pre_load()
    def format_date(self, data):
        data['birthdate'] = datetime.datetime.strptime(data['birthdate'],'%m/%d/%Y').date().isoformat()

target = os.path.dirname(os.path.realpath(__file__)) + "/example.csv"       # target file from which to extract data (in this case, it's a local file)

package_id = '83ba85c6-9fd5-4603-bd98-cc9002e206dc'     # GUID of the CKAN packagae(dataset) that the resource is part of
resource_name = 'Example Data'                          # Name of resource within that package
api_key = 'ApiKeyOf-CKAN-User-With-Privileges00'        # API key of user with write privileges to create
ckan_url = 'https://data.wprdc.org/'


status_db = os.path.dirname(os.path.realpath(__file__)) + "/status.db"

example_pipeline = pl.Pipeline('example_pipeline', 'Example Pipeline',
                               log_status=True, conn_name='./status.db',
                               settings_from_file=False) \
    .connect(pl.FileConnector, target) \
    .extract(pl.CSVExtractor) \
    .schema(ExampleSchema) \
    .load(pl.CKANDatastoreLoader, 'ckan',
          fields=ExampleSchema().serialize_to_ckan_fields(),
          package_id=package_id,
          resource_name=resource_name,
          method='insert',
          ckan_api_key=api_key,
          ckan_root_url=ckan_url
          )
```
### Docs

Documentation is stored in the docs directory. To make and view docs locally, run the following (on a mac):

```bash
cd docs
make html
open _build/html/index.html
```

A make.bat file is included for windows users as well.

