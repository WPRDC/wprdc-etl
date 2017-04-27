from marshmallow import Schema, fields

FIELD_TO_CKAN_TYPE_MAPPING = {
    fields.String: 'text',
    fields.Number: 'numeric', fields.Integer: 'int',
    fields.DateTime: 'timestamp', fields.Date: 'date',
    fields.Float: 'float', fields.Boolean: 'bool',
    fields.Time: 'time', fields.JSON: 'json'
}

class BaseSchema(Schema):
    '''Base schema for the pipeline. Extends :py:class:`marshmallow.Schema`
    '''

    def serialize_to_ckan_fields(self, capitalize=False):
        '''Convert schema fieldlist to CKAN-friendly Fields

        Returns:
            A list of dictionaries with proper name/type mappings
            for CKAN. For example, name=fields.String() would go
            to:

            .. code-block:: json

                [
                    {
                        'id': 'name',
                        'type': 'text'
                    }
                ]
        '''
        ckan_fields = []
        for name, marsh_field in self.fields.items():
            if marsh_field.load_only:
                continue
            if marsh_field.dump_to is not None:
                name = marsh_field.dump_to
            name = name.upper() if capitalize else name
            ckan_fields.append({
                'id': name,
                'type': FIELD_TO_CKAN_TYPE_MAPPING[marsh_field.__class__]
            })
        return ckan_fields
