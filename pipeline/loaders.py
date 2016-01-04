import os
import requests
import json
import datetime

HERE = os.path.abspath(os.path.dirname(__file__))
PARENT = os.path.join(HERE, '..')

class InvalidConfigException(Exception):
    pass

class Loader(object):
    def load(self, data):
        raise NotImplementedError

class Datapusher(Loader):
    """Connection to ckan datastore"""
    def __init__(self, server="staging", settings_file=None):
        f = None
        settings_file = settings_file if settings_file else \
            os.path.join(PARENT, 'settings.json')
        try:
            f = open(settings_file, 'r')
            raw_config = json.loads(f.read())
            self.config = raw_config[server]
        except (KeyError, IOError):
            raise InvalidConfigException(
                'No config file found, or config not properly formatted'
            )
        finally:
            if f:
                f.close()

        self.ckan_url = self.config['root_url'].rstrip('/') + '/api/3/'
        self.dump_url = self.config['root_url'].rstrip('/') + '/datastore/dump/'
        self.key = self.config['api_key']

    def resource_exists(self, package_id, resource_name):
        """
        Searches for resource on ckan instance

        Params:
            package_id: id of resources parent dataset
            resource_name: name of the resource

        Returns:
            ``True`` if the resource is found within the package,
            ``False`` otherwise
        """

        check_resource = requests.post(
            self.ckan_url + 'action/package_show',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'id': package_id
            })
        )

        response = check_resource.json()
        return resource_name in set(i['name'] for i in response['result']['resources'])

    def create_resource(self, package_id, resource_name):
        '''
        Creates new resource in ckan instance

        Params:
            package_id: dataset under which to add new resource
            resource_name: name of new resource

        Returns:
            id of newly created resource if successful, None otherwise
        '''

        # Make api call
        create_resource = requests.post(
            self.ckan_url + 'action/resource_create',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'package_id': package_id,
                'url': '#',
                'name': resource_name,
                'url_type': 'datapusher',
                'format': 'CSV'
            })
        )

        resource = create_resource.json()

        if not resource.get('success', False):
            print("ERROR: " + resource['error']['name'][0])
            return

        print("SUCCESS: Resource #" + str(resource['result']['id']) + ' was created.')
        return resource['result']['id']

    def create_datastore(self, resource_id, fields):
        """
        Creates new datastore for specified resource

        Params:
            resource_id: resource id for which new datastore is being made
            fields: header fields for csv file

        Returns:
            resource_id for the new datastore if successful
        """

        # Make API call
        create_datastore = requests.post(
            self.ckan_url + 'action/datastore_create',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'resource_id': resource_id,
                'force': True,
                'fields': fields
            })
        )

        create_datastore = create_datastore.json()

        if not create_datastore.get('success', False):
            print("ERROR: " + create_datastore['error']['name'][0])
            return

        print(
            'SUCCESS: Datastore #{} was created.'.format(
                create_datastore['result']['resource_id']
            )
        )

        return create_datastore['result']['resource_id']

    def delete_datastore(self, resource_id):
        """
        Deletes datastore table for resource

        Params:
            resource: resource_id to remove table from

        Returns:
            Status code from the request
        """
        delete = requests.post(
            self.ckan_url + 'action/datastore_delete',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'resource_id': resource_id,
                'force': True
            })
        )
        return delete.status_code

    def upsert(self, resource_id, data):
        """
        Upsert data into datastore

        Params:
            resource_id: resource_id to which data will be inserted
            data: data to be upserted

        Returns:
            request status
        """
        insert = requests.post(
            self.ckan_url + 'action/datastore_upsert',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'resource_id': resource_id,
                'method': 'insert',
                'force': True,
                'records': data
            })
        )
        return insert.status_code

    def update_metadata(self, resource_id):
        """
        TODO: Make this versatile

        Params:
            resource_id: resource whose metadata willbe modified

        Returns:
            request status
        """
        update = requests.post(
            self.ckan_url + 'action/resource_patch',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'id': resource_id,
                'url': self.dump_url + str(resource_id),
                'url_type': 'datapusher',
                'last_modified': datetime.datetime.now().isoformat(),
            })
        )
        return update.status_code

    def load(self, data):
        pass
