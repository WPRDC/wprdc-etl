from marshmallow import Schema, fields

FIELD_TO_CKAN_TYPE_MAPPING = {
    fields.String: 'text',
    fields.Number: 'numeric', fields.Integer: 'numeric',
    fields.DateTime: 'timestamp', fields.Date: 'timestamp'
}

class BaseSchema(Schema):
    def serialize_to_ckan_fields(self):
        '''Convert schema fieldlist to CKAN-friendly Fields

        Returns:
            A list of dictionaries with proper name/type mappings
            for CKAN. For example, name=fields.String() would go
            to:
                [
                    {
                        'id': 'name',
                        'type': 'text'
                    }
                ]
        '''
        ckan_fields = []
        for name, marsh_field in self.fields.items():
            ckan_fields.append({
                'id': name,
                'type': FIELD_TO_CKAN_TYPE_MAPPING[marsh_field.__class__]
            })
