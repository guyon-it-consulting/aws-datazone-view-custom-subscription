import datetime
import json
import logging
import os

import boto3

datazone_client = boto3.client('datazone')
events_client = boto3.client('events')

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGGER_LEVEL", "INFO"))


# Define a custom function to serialize objects
def custom_serializer(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")


def retrieve_listing_infos(domain_id, listing_id, version):
    resp = datazone_client.get_listing(
        domainIdentifier=domain_id,
        identifier=listing_id,
        listingRevision=version,
    )

    return resp['item']['assetListing']


def retrieve_environment(domain_id, environment_id):
    resp = datazone_client.get_environment(
        domainIdentifier=domain_id,
        identifier=environment_id
    )
    return resp


def retrieve_environments_by_project(domain_id, project_id):
    environments = []

    paginator = datazone_client.get_paginator('list_environments')

    page_iterator = paginator.paginate(
        domainIdentifier=domain_id,
        projectIdentifier=project_id,
        status='ACTIVE'
    )

    for page in page_iterator:
        environments += page['items']

    return environments


def lambda_handler(event, context):
    logger.info(event)

    # only if isManagedAsset=false

    if event['detail']['data']['isManagedAsset']:
        logger.info("This is a managed asset - ignore it")
        return

    # get domain info
    domain_id = event['detail']['metadata']['domain']
    logger.info(f"Domain is {domain_id}")

    subscribed_envs = []

    # find subscription project / accounts / environments
    for subscribed_principal in event['detail']['data']['subscribedPrincipals']:
        if subscribed_principal['type'] != 'PROJECT':
            logger.warning(f"This is not a supported subscription principal type {subscribed_principal}")
            continue

        subscribed_project_id = subscribed_principal['id']

        # find all environment part of  this project
        # TODO ensure this is Athena blueprint
        for subscribed_environment in retrieve_environments_by_project(domain_id, subscribed_project_id):
            aws_account_id = subscribed_environment['awsAccountId']
            region = subscribed_environment['awsAccountRegion']
            environment_id = subscribed_environment['id']

            environment = retrieve_environment(domain_id, environment_id)

            user_role_arn = None
            glue_consumer_db_name = None

            for provisionedResource in environment['provisionedResources']:
                if provisionedResource['name'] == "userRoleArn":
                    user_role_arn = provisionedResource['value']
                if provisionedResource['name'] == "glueConsumerDBName":
                    glue_consumer_db_name = provisionedResource['value']

            subscribed_envs.append({
                "awsAccountId": aws_account_id,
                "region": region,
                "environmentId": environment_id,
                "athenaUserRoleArn": user_role_arn,
                "glueConsumerDBName": glue_consumer_db_name,
            })

    for listing in event['detail']['data']['subscribedListings']:
        listing_infos = retrieve_listing_infos(domain_id, listing['id'], listing['version'])

        # Retrieve producer infos from the subscription listing
        listing_infos_details = json.loads(listing_infos['forms'])

        producer_aws_account_id = listing_infos_details['GlueViewForm']['catalogId']
        producer_region = listing_infos_details['GlueViewForm']['region']

        forwarded_event = {
            "data": event['detail']['data'],
            "asset": {
                # "databaseName": listing_infos_details['GlueViewForm']['databaseName'],
                "tableName": listing_infos_details['GlueViewForm']['tableName'],
                "tableArn": listing_infos_details['GlueViewForm']['tableArn'],
                "query": listing_infos_details['GlueViewForm']['query']
            },
            "subscriptions": subscribed_envs
        }

        target_event_bus = f"arn:aws:events:{producer_region}:{producer_aws_account_id}:event-bus/{os.environ['EVENT_BUS_NAME']}"
        logger.info(f"Pushing event to {target_event_bus}")

        resp = events_client.put_events(
            Entries=[{
                'Source': os.environ['EVENT_SOURCE'],
                # 'DetailType': event['detail-type'],
                'DetailType': 'Unmanaged Asset Subscription Request Accepted',
                'Detail': json.dumps(forwarded_event, default=custom_serializer),
                'EventBusName': target_event_bus
            }]
        )

        if resp['FailedEntryCount'] > 0:
            logger.error(resp['Entries'])
            raise Exception("Failed to push event")
