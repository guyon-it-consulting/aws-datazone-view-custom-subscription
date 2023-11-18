import datetime
import json
import logging
import os

import boto3

datazone_client = boto3.client('datazone')
events_client = boto3.client('events')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# Define a custom function to serialize objects
def custom_serializer(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")


def retrieve_listing_infos(domain_id, id, version):
    resp = datazone_client.get_listing(
        domainIdentifier=domain_id,
        identifier=id,
        listingRevision=version,
    )

    return resp['item']['assetListing']


def retrieve_environment(domain_id, id):
    resp = datazone_client.get_environment(
        domainIdentifier=domain_id,
        identifier=id
    )
    return resp


def lambda_handler(event, context):
    logger.info(event)

    # only if isManagedAsset=false

    if event['detail']['data']['isManagedAsset']:
        logger.info("This is a managed asset - ignore it")
        return

    # get entity info
    domain_id = event['detail']['metadata']['domain']
    logger.info(f"Domain is {domain_id}")

    for listing in event['detail']['data']['subscribedListings']:
        listing_infos = retrieve_listing_infos(domain_id, listing['id'], listing['version'])

        listing_infos_details = json.loads(listing_infos['forms'])
        aws_account_id = listing_infos_details['GlueViewForm']['catalogId']
        region = listing_infos_details['GlueViewForm']['region']
        environment_id = \
        listing_infos_details['DataSourceReferenceForm']['dataSourceIdentifier']['DataSourceCommonForm'][
            'environmentId']
        logger.info(f"Environment is {environment_id}, requesting")
        environment_infos = retrieve_environment(domain_id, environment_id)

        # Build forwarded event TODO create a dedicated one
        forwarded_event = event['detail']
        # add listing infos to the event
        event['detail']['listing'] = listing_infos
        event['detail']['environment'] = environment_infos

        target_event_bus = f"arn:aws:events:{region}:{aws_account_id}:event-bus/{os.environ['EVENT_BUS_NAME']}"
        logger.info(f"Pushing event to {target_event_bus}")

        resp = events_client.put_events(
            Entries=[{
                'Source': os.environ['EVENT_SOURCE'],
                'DetailType': event['detail-type'],
                'Detail': json.dumps(forwarded_event, default=custom_serializer),
                'EventBusName': target_event_bus
            }]
        )

        if resp['FailedEntryCount'] > 0:
            logger.error(resp['Entries'])
            raise Exception("Failed to push event")
