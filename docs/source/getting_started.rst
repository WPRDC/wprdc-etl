Getting Started
===============

.. note::
    This is likely to change as the project becomes available on Pypi.

For Writing Pipelines
---------------------

If all you want is to use the codebase to write additional pipelines and don't want to develop on the core codebase itself, the following steps should be enough to get started. It is highly recommended that you use `virtualenv <http://virtualenv.readthedocs.org/en/latest/>`_

.. code-block:: bash

    git clone https://github.com/UCSUR-Pitt/wprdc-etl
    cd wprdc-etl
    # install dependencies. if you just need access the Pipeline and related
    # classes, you can install via setup.py:
    pip install -e .
    # if you want to develop, you'll need to install additional requirements
    # (such as the test runner):
    pip install -r requirements.txt
    # create a copy of the settings
    cp settings.json.example settings.json
    # after the settings have been copied, you will need to go in and
    # edit the various values for your setup. at this point, you should
    # be ready to create the status database. by installing the package above
    # you should have access to two command-line commands, one of which
    # will create the database for you
    create_monitoring_db settings.json --server <name of your server>
    # note, if you need to destroy and recreate the status data at any point
    # you can do so with:
    # create_monitoring_db --server <name of your server> --drop

For Developers
--------------

If you want to develop the codebase and do things such as run tests and build documentation, you can follow the same steps, except you'll need to install additional development dependencies (such as nosetests and sphinx). The commands for all of that is reproduced below:

.. code-block:: bash

    git clone https://github.com/UCSUR-Pitt/wprdc-etl
    cd wprdc-etl
    pip install -e .
    pip install -r requirements.txt
    cp settings.json.example settings.json
    create_monitoring_db settings.json --server <name of your server>
