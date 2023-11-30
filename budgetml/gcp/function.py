import logging
import os
import zipfile
from tempfile import TemporaryFile

import googleapiclient.discovery
import requests

from budgetml import autostarter
from budgetml.gcp.pubsub import create_topic


def get_api():
    service = googleapiclient.discovery.build('cloudfunctions', 'v1')
    return service.projects().locations().functions()


def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            if file != '__init__.py':
                ziph.write(os.path.join(root, file), file)


def create_upload_url(parent):
    upload_url = \
        get_api().generateUploadUrl(parent=parent,
                                    body={}).execute()[
            'uploadUrl']
    logging.debug("Create Upload URL", upload_url)

    with TemporaryFile() as data:
        with zipfile.ZipFile(data, 'w', zipfile.ZIP_DEFLATED) as archive:
            zipdir(autostarter.__path__[0], archive)
        data.seek(0)
        headers = {
            'content-type': 'application/zip',
            'x-goog-content-length-range': '0,104857600'
        }
        logging.debug("Create Upload URL",
                      requests.put(upload_url, headers=headers, data=data))
    return upload_url


def create_cloud_function(
        project,
        region,
        function_name,
        instance_zone,
        instance_name,
        topic,
        timeout=200):
    # create pubsub topic
    full_topic = create_topic(project, topic)

    parent = f'projects/{project}/locations/{region}'

    upload_url = create_upload_url(parent)
    config = {
        "name": f'{parent}/functions/{function_name}',
        "entryPoint": "launch",
        "runtime": "python37",
        "availableMemoryMb": 128,
        "timeout": f"{timeout}s",
        "environmentVariables": {
            "BUDGET_PROJECT": project,
            "BUDGET_ZONE": instance_zone,
            "BUDGET_INSTANCE": instance_name,
        },
        "sourceUploadUrl": upload_url,
        "eventTrigger": {
            "eventType": "providers/cloud.pubsub/eventTypes/topic.publish",
            "resource": f"{full_topic}",
        },
    }

    logging.debug(f'Creating function with config: {config}')
    res = get_api().create(
        location=parent,
        body=config).execute()
    logging.debug(f'Function {function_name} created. Response: {res}')
    return res


def delete_cloud_function(project, region, function_name):
    parent = f'projects/{project}/locations/{region}'
    full_name = f'{parent}/functions/{function_name}'
    res = get_api().delete(
        name=full_name).execute()
    logging.debug(f'Function {function_name} deleted. Response: {res}')
    return res
