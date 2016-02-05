from operator import itemgetter
from unittest import TestCase

import pipeline as pl
from marshmallow import fields

class FakeSchema(pl.BaseSchema):
    str = fields.String()
    int = fields.Integer()
    num = fields.Number()
    datetime = fields.DateTime()
    date = fields.Date(dump_to='a_different_name')
    not_there = fields.String(load_only=True)

class TestSchema(TestCase):
    def test_ckan_serialization(self):
        fields = FakeSchema().serialize_to_ckan_fields()
        self.assertListEqual(
            sorted(fields, key=itemgetter('id')),
            [
                {'id': 'a_different_name', 'type': 'date'},
                {'id': 'datetime', 'type': 'timestamp'},
                {'id': 'int', 'type': 'numeric'},
                {'id': 'num', 'type': 'numeric'},
                {'id': 'str', 'type': 'text'}
            ]
        )

    def test_ckan_serialization_caps(self):
        fields = FakeSchema().serialize_to_ckan_fields(capitalize=True)
        self.assertListEqual(
            sorted(fields, key=itemgetter('id')),
            [
                {'id': 'A_DIFFERENT_NAME', 'type': 'date'},
                {'id': 'DATETIME', 'type': 'timestamp'},
                {'id': 'INT', 'type': 'numeric'},
                {'id': 'NUM', 'type': 'numeric'},
                {'id': 'STR', 'type': 'text'}
            ]
        )
