import requests
import json
import datetime


class Datapusher(object):
    """Connection to ckan datastore"""

    def __init__(self, server="staging", settings_file='../settings.json'):
        raw_config = json.load(open(settings_file))
        self.config = raw_config[server]

        self.ckan_url = self.config['root_url'].rstrip('/') + '/api/3/'
        self.dump_url = self.config['root_url'].rstrip('/') + '/datastore/dump/'
        self.key = self.config['api_key']

    def resource_exists(self, packageid, resource_name):
        """
        Searches for resource on ckan instance
        :param packageid: id of resources parent dataset
        :param resource_name:  resources name
        :return: true if found false otherwise
        """

        check_resource = requests.post(
            self.ckan_url + 'action/package_show',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'id': packageid
            })
        )
        import pdb; pdb.set_trace()
        check = json.loads(check_resource.content)

        # Check if this month's resource already exists
        return resource_name in [i['name'] for i in check['result']['resources']]


    def create_resource(self, packageid, resource_name):
        """
        Creates new resource in ckan instance
        :param packageid: dataset under which to add new resource
        :param resource_name: name of new resource
        :return: id of newly created resource if successful
        """

        # Make api call
        create_resource = requests.post(
            self.ckan_url + 'action/resource_create',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'package_id': packageid,
                'url': '#',
                'name': resource_name,
                'url_type': 'datapusher',
                'format': 'CSV'
            })
        )

        resource = json.loads(create_resource.text)

        if not resource['success']:
            print("ERROR: " + resource['error']['name'][0])
            return

        print("SUCCESS: Resource #" + resource['result']['id'] + ' was created.')
        return resource['result']['id']

    def create_datastore(self, resource, fields):
        """
        Creates new datastore for specified resource
        :param resource: resource id fo which new datastore is being made
        :param fields: header fields for csv file
        :return: resource id if successful
        """

        # Make API call
        datastore_creation = requests.post(
            self.ckan_url + 'action/datastore_create',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'resource_id': resource,
                'force': True,
                'fields': fields
            })
        )

        datastore = json.loads(datastore_creation.text)

        if not datastore['success']:
            print("ERROR: " + datastore['error']['name'][0])
            return
        print("SUCCESS: Datastore #" + datastore['result']['resource_id'] + ' was created.')
        return datastore['result']['resource_id']

    def delete_datastore(self, resource):
        """
        Deletes datastore table for resource
        :param resource: resource to remove table from
        :return: request status
        """
        delete = requests.post(
            self.ckan_url + 'action/datastore_delete',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'resource_id': resource,
                'force': True
            })
        )
        return delete.status_code

    def upsert(self, resource, data):
        """
        Upsert data into datastore
        :param resource: resource to which data will be inserted
        :param data: data to be upserted
        :return: request status
        """
        insert = requests.post(
            self.ckan_url + 'action/datastore_upsert',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'resource_id': resource,
                'method': 'insert',
                'force': True,
                'records': data
            })
        )
        return insert.status_code

    def update_meta_data(self, resource):
        """
        TODO: Make this versatile
        :param resource: resource whose metadata willbe modified
        :return: request status
        """
        update = requests.post(
            self.ckan_url + 'action/resource_patch',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'id': resource,
                'url': self.dump_url + resource,
                'url_type': 'datapusher',
                'last_modified': datetime.datetime.now().isoformat(),
            })
        )
        return update.status_code

    def resource_search(self, name):
        """

        :param name:
        :return:
        """
        search = requests.post(
            self.ckan_url + 'action/datastore_search',
            headers={
                'content-type': 'application/json',
                'authorization':self.key
            },
            data=json.dumps({
                "resource_id": name,
                "plain": False
            })
        )

        return search

