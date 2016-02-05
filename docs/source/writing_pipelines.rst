An Example Pipeline
===================

The primary goal is to make it as easy as possible to write new pipelines. The main additional work that is required to write a new pipeline is to construct a new Schema. Here, we'll deconstruct an example to see some of the tricks that can be used, especially when building the schema.

Building the schema
-------------------

Writing pipelines a matter of understanding where your data is coming from, and writing a schema to handle it. Let's take the Pittsburgh Police Blotter dataset and build an example schema from it. The raw source data is available from `the Police website <http://communitysafety.pittsburghpa.gov/Blotter.aspx>`_.

.. code-block:: python

    from marshmallow import pre_load, post_load, fields
    import pipeline as pl

    class PoliceBlotterSchema(pl.BaseSchema):
        report_name = fields.String()
        ccr = fields.Integer()
        section = fields.String()
        description = fields.String()
        arrest_date = fields.DateTime(format='%m/%d/%Y', load_only=True)
        arrest_time = fields.DateTime(format='%H:%M')
        address = fields.String(allow_none=True)
        neighborhood = fields.String(allow_none=True)
        zone = fields.Integer(allow_none=True)
        age = fields.Integer(allow_none=True)
        gender = fields.String(allow_none=True)

        @pre_load
        def process_na_zone(self, data):
            zone = data.get('zone')
            if zone.lower() == 'n/a':
                data['zone'] = None
            return data

        @post_load
        def combine_date_and_time(self, in_data):
            in_data['arrest_time'] = str(datetime.datetime(
                in_data['arrest_date'].year, in_data['arrest_date'].month,
                in_data['arrest_date'].day, in_data['arrest_time'].hour,
                in_data['arrest_time'].minute, in_data['arrest_time'].second
            ))

There are a few notable things going on here, specifically with the :py:func:`~marshmallow.decorators.pre_load` and :py:func:`~marshmallow.decorators.post_load` decorators.

``@pre_load`` will register a method to be run on the raw data before it is processed by the Schema's main :py:meth:`~marshmallow.Schema.load` method. In this case, we set a custom 'n/a' designation to ``None``.

``@post_load``, as you might expect, registers a method to run **after** the deserialization of an object and the schema is constructed. In this case, we take our ``arrest_time`` field and combine it with the ``arrest_date`` field to make one condensed date field. Because our loaded ``arrest_date`` field is now a duplicate, we can ensure that it isn't passed on to our ``loader`` by using the ``load_only`` flag in the field. Similarly, if we wanted to create a new field from our deserialized data, we could use the ``dump_only`` flag and ensure that field only exists in the dumped data.

Writing the rest of the pipeline
--------------------------------

Now that we have our schema sorted out, we can go ahead and write the rest of our pipeline. Note that we will be using a :py:class:`~pipeline.connectors.RemoteFileConnector` to connect to yesteryday's raw data.

.. code-block:: python

    import datetime
    import pipeline as pl

    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1))
    url = 'http://apps.pittsburghpa.gov/police/arrest_blotter/arrest_blotter_%s.csv' % (yesterday.strftime("%A"))

    police_blotter_pipeline = pl.Pipeline('police_blotter_pipeline', 'Police Blotter Pipeline') \
        .connect(pl.RemoteFileConnector, url) \
        .extract(pl.CSVExtractor) \
        .schema(PoliceBlotterSchema) \
        .load(pl.CKANDatastoreLoader,
              fields=PoliceBlotterSchema().serialize_to_ckan_fields(capitalize=True),
              package_id='your-ckan-package-id-here'
              resource_name='Incidents',
              method='insert') \
        .run()

Let's deconstruct what's going on here. As we can see, the steps are all laid out in the primary methods:

    1. First, we connect to the remote file's location, which is generated from a base URL, string interpolated with the name of the previous day of the week. If necessary, we could override the file's encoding to ensure that it opens as expected.
    2. Secondly, we note that this is going to be a comma-separated input source. If necessary, we could override the delimiter if it were tab- or pipe-separated data. We could also pass in custom headers. One thing to note here is that headers are automatically mapped to ``schema_headers``, which are the names of the marshmallow fields in our schema. If headers are manually passed, they need match the schema headers so that marshmallow knows what to look for. Optionally, the individual marshmallow fields can also take a ``load_from`` field which allows headers to be mapped on a per-field level.
    3. Our above-specified schema is passed to the pipeline.
    4. We specify that we are going to use a :py:class:`~pipeline.loaders.CKANDatastoreLoader`. This has some required kwargs, which include the data insertion method (must be either ``insert`` or ``upsert``), and the ``fields`` to use. There is a :py:meth:`~pipeline.schema.BaseSchema.serialize_to_ckan_fields` convenience method attached to all pipeline schema, which will automatically build the required ``fields`` in the correct format for CKAN.
    5. Finally, with everything declared, we run the pipeline!
