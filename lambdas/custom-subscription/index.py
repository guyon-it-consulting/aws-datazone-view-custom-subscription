import json
import logging
import re
import boto3
import datetime
import os
import base64
from sql_metadata import Parser
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGGER_LEVEL", "INFO"))

sts_client = boto3.client('sts')
iam_client = boto3.client('iam')
glue_client = boto3.client('glue')
lf_client = boto3.client('lakeformation')
events_client = boto3.client('events')


# Define a custom function to serialize objects
def custom_serializer(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")


def get_current_account_id():
    return sts_client.get_caller_identity()['Account']


def get_current_principal_identifier():
    caller_arn = sts_client.get_caller_identity()['Arn']
    logger.info(f"callerArn: {caller_arn}")

    import re
    match = re.search(r'^arn:aws:sts::(\d+):assumed-role/([\w-]+)/([\w-]+)$', caller_arn)
    if match:
        role_name = match.group(2)
        role_arn = iam_client.get_role(RoleName=role_name)['Role']['Arn']
        return role_arn
    else:
        # TODO do better
        return ""


def grant_all_on_database(catalog_id, database_name, principal_arn):
    logger.info(f"granting all on {database_name} to {principal_arn} in {catalog_id}")

    try:
        lf_client.grant_permissions(
            Principal={
                'DataLakePrincipalIdentifier': principal_arn
            },
            Resource={
                'Database': {
                    'CatalogId': catalog_id,
                    'Name': database_name
                }
            },
            Permissions=[
                'ALL'
            ],
            PermissionsWithGrantOption=[
                'ALL'
            ]
        )
    except ClientError as e:
        logger.error(e)
        raise Exception(e)


def grant_read_on_database(catalog_id, database_name, principal_arn, allows_grants=False):
    logger.info(f"granting {database_name} to {principal_arn} in {catalog_id}")

    permission_with_grant_option = []
    if allows_grants:
        permission_with_grant_option = [
            'DESCRIBE',
        ]

    try:
        lf_client.grant_permissions(
            Principal={
                'DataLakePrincipalIdentifier': principal_arn
            },
            Resource={
                'Database': {
                    'CatalogId': catalog_id,
                    'Name': database_name
                }
            },
            Permissions=[
                'DESCRIBE',
            ],
            PermissionsWithGrantOption=permission_with_grant_option
        )
    except ClientError as e:
        logger.error(e)
        raise Exception(e)


# Grant Read on a given table
def grant_read_on_table(catalog_id, database_name, table_name, principal_arn, allows_grants=False):
    logger.info(f"granting {database_name}.{table_name} to {principal_arn} in {catalog_id}")

    permission_with_grant_option = []
    if allows_grants:
        permission_with_grant_option = [
            'SELECT',
            'DESCRIBE',
        ]

    try:
        lf_client.grant_permissions(
            Principal={
                'DataLakePrincipalIdentifier': principal_arn
            },
            Resource={
                'Table': {
                    'CatalogId': catalog_id,
                    'DatabaseName': database_name,
                    'Name': table_name
                }
            },
            Permissions=[
                'SELECT',
                'DESCRIBE',
            ],
            PermissionsWithGrantOption=permission_with_grant_option
        )
    except ClientError as e:
        logger.error(e)
        raise Exception(e)


# Grant Read on a resource link table
def grant_read_on_resource_link(catalog_id, database_name, table_name, principal_arn):
    logger.info(f"granting {database_name}.{table_name} to {principal_arn} in {catalog_id}")

    try:
        lf_client.grant_permissions(
            Principal={
                'DataLakePrincipalIdentifier': principal_arn
            },
            Resource={
                'Table': {
                    'CatalogId': catalog_id,
                    'DatabaseName': database_name,
                    'Name': table_name
                }
            },
            Permissions=[
                'DESCRIBE',
            ]
        )
    except ClientError as e:
        logger.error(e)
        raise Exception(e)


# TODO refactor
def grant_all_on_table(catalog_id, database_name, table_name, principal_arn):
    logger.info(f"granting ALL {database_name}.{table_name} to {principal_arn} in {catalog_id}")

    try:
        lf_client.grant_permissions(
            Principal={
                'DataLakePrincipalIdentifier': principal_arn
            },
            Resource={
                'Table': {
                    'CatalogId': catalog_id,
                    'DatabaseName': database_name,
                    'Name': table_name
                }
            },
            Permissions=[
                'ALL'
            ],
            PermissionsWithGrantOption=[
                'ALL'
            ]
        )
    except ClientError as e:
        logger.error(e)
        raise Exception(e)


def create_resource_link_table(database, table_name, target_database, target_table_name, target_account_id,
                               target_region):
    try:
        glue_client.create_table(
            DatabaseName=database,
            TableInput={
                'Name': table_name,
                'TargetTable': {
                    'DatabaseName': target_database,
                    'Name': target_table_name,
                    'CatalogId': target_account_id,
                    'Region': target_region
                }
            }
        )
        logger.info("Resource Link created")
    except glue_client.exceptions.AlreadyExistsException as e:
        logger.warning('Resource Link already existing, not need to recreate it...')
        pass


def create_resource_link_database(database_name, target_database, target_account_id, target_region):
    try:
        glue_client.create_database(
            DatabaseInput={
                'Name': database_name,
                'TargetDatabase': {
                    'DatabaseName': target_database,
                    'CatalogId': target_account_id,
                    'Region': target_region
                }
            }
        )
        logger.info("Resource Link created")
    except glue_client.exceptions.AlreadyExistsException as e:
        logger.warning('Resource Link already existing, not need to recreate it...')
        pass


def ensure_role_has_extended_policy(role_arn):
    policy_document = 'DatazoneCustomSubscription'
    role_name = role_arn.split('/')[-1]

    has_already_policy = False
    try:
        # add needed IAM grants
        iam_client.get_role_policy(
            RoleName=role_name,
            PolicyName=policy_document
        )
        has_already_policy = True
    except iam_client.exceptions.NoSuchEntityException:
        pass

    if not has_already_policy:
        iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName=policy_document,
            PolicyDocument='''{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetTables",
        "glue:GetPartition",
        "glue:BatchGetPartition"
      ],
      "Resource": [
        "*"
      ]
    }
  ]
}'''
        )
        logger.info("Put Extended role")


# Cache of glue:GetTable responses, aim to speed up the analysis view process
cache_get_table = dict()


# retrieve View and its SQL
def get_view_sql(database_name, view_name):
    if f"{database_name}.{view_name}" in cache_get_table:
        resp = cache_get_table[f"{database_name}.{view_name}"]

        if str(resp).startswith("ERROR_"):
            return "error", None
    else:
        try:
            resp = glue_client.get_table(DatabaseName=database_name, Name=view_name)
            cache_get_table[f"{database_name}.{view_name}"] = resp
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                logger.warning(
                    f"WARN: it look likes {database_name}.{view_name} is not a view nor a table, maybe a parsing error?")
                cache_get_table[f"{database_name}.{view_name}"] = f"ERROR_{e.response['Error']['Code']}"
            else:
                logger.error(e)
            return "error", None

    if not resp['Table']['TableType'] == 'VIRTUAL_VIEW':
        return "table", None

    view_original_text = resp['Table']['ViewOriginalText']

    base64_regex = "(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?"

    result = re.search(rf"^/\*\sPresto\sView:\s({base64_regex})\s\*/$", view_original_text)
    base64_query = result.group(1)
    raw = base64.b64decode(base64_query.encode('ascii')).decode('ascii')
    dumps = json.loads(raw)

    clear_sql = dumps['originalSql']

    return "view", clear_sql


# Analyze view in order to find all its dependencies
def analyze_view(database_name, name):
    glue_type, clear_sql = get_view_sql(database_name, name)

    if glue_type == 'error':
        return None

    deps_glue_tables = set()

    if clear_sql is not None:
        parser = Parser(clear_sql)

        deps = set(parser.tables)

        for t in deps:
            split = t.split('.')
            if len(split) == 1:  # TODO can we consider that not fully qualify data is not a table or view reference?
                next_database_name = database_name
                next_table_name = split[0]
            else:
                next_database_name = split[0]
                next_table_name = split[1]

            inner_deps = analyze_view(next_database_name, next_table_name)

            if inner_deps is not None:
                deps_glue_tables |= inner_deps
                deps_glue_tables.add(f"{next_database_name}.{next_table_name}")

    return deps_glue_tables


def handle_unmanaged_asset_subscription_on_producer(event):
    logger.info('Hande subscription')
    if event['detail']['data']['isManagedAsset']:
        logger.info("This is a managed asset - ignore it")
        return

        # get subscriptionsM
    subscriptions_principals_accounts = set([subscription['awsAccountId'] for subscription in
                                             event['detail']['subscriptions']])

    if current_account_id in subscriptions_principals_accounts:
        logger.info("One of the subscription environment is in the current - producer environment - account")
        logger.info("it has to be omitted from lakeformation/ram cross-account sharing")
        subscriptions_principals_accounts.remove(current_account_id)

    logger.debug(f"Roles for Subscription {subscriptions_principals_accounts}")

    table_arn = event['detail']['asset']['tableArn']
    # table_name = event['detail']['asset']['tableName']
    # database = event['detail']['asset']['databaseName']

    result = re.search(r"^arn:aws:glue:(\w+-\w+-\d):(\d+):table/(\w+)/(\w+)$", table_arn)
    if not result:
        raise Exception("Cannot parse table arn")

    target_database = result.group(3)
    table_name = result.group(4)

    # account_id = result.group(2)
    # region = result.group(1)

    logger.info(f"Handle table {table_name} in Database: {target_database}")

    # not Needed because ALL_TABLES SELECT/DESCRIBE grant by default in DataZone provisioning
    # # Grant this resource link to datazone user
    # grant_table(consumer_database, table_name, user_role_arn)

    # Grant underlying view to datazone account principal
    for principal in subscriptions_principals_accounts:
        # GRANTS at account/catalog level with SELECT/DESCRIBE and GRANT-ABLES
        grant_read_on_table(current_account_id, target_database, table_name, principal, True)
        grant_read_on_database(current_account_id, target_database, principal, True)

    # Analyze view, and find dependents views & tables that needs to be granted to datazone user
    # convert to array in order to be JSON serializable
    dependent_table_or_views = []
    for item in list(analyze_view(target_database, table_name)):
        split = item.split('.')
        dependent_table_or_views.append({"database_name": split[0], "table_name": split[1]})
        # TODO add catalog id and/or arn

    # Grant tables
    for dependent_item in dependent_table_or_views:
        logger.info(f"Dependent resource found {dependent_item}")
        # grant this dependent table to datazone account principal
        for principal in subscriptions_principals_accounts:
            grant_read_on_table(
                current_account_id,
                dependent_item['database_name'],
                dependent_item['table_name'],
                principal,
                True
            )
    # Grant databases
    for database_name in set([dependent_item['database_name'] for dependent_item in dependent_table_or_views]):
        for principal in subscriptions_principals_accounts:
            grant_read_on_database(
                current_account_id,
                database_name,
                principal,
                True
            )

    # SEND AN EVENT for each Subscription
    for subscription in event['detail']['subscriptions']:
        consumer_region = subscription['region']
        consumer_aws_account_id = subscription['awsAccountId']

        target_event_bus = f"arn:aws:events:{consumer_region}:{consumer_aws_account_id}:event-bus/{os.environ['EVENT_BUS_NAME']}"
        logger.info(f"Pushing event to {target_event_bus}")

        detail_event = {
            "data": event['detail']['data'],
            "asset": event['detail']['asset'],
            "subscription": subscription,
            "glueDependencies": dependent_table_or_views,
        }

        logger.info(detail_event)

        resp = events_client.put_events(
            Entries=[{
                'Source': os.environ['EVENT_SOURCE'],
                'DetailType': "Unmanaged Asset Successfully Granted in Pub Environment",
                'Detail': json.dumps(detail_event, default=custom_serializer),
                'EventBusName': target_event_bus
            }]
        )

        if resp['FailedEntryCount'] > 0:
            logger.error(resp['Entries'])
            raise Exception("Failed to push event")


def handle_unmanaged_asset_subscription_on_consumer(event):
    logger.info('Hande Unmanaged asset subscription on consumer')

    table_arn = event['detail']['asset']['tableArn']

    result = re.search(r"^arn:aws:glue:(\w+-\w+-\d):(\d+):table/(\w+)/(\w+)$", table_arn)
    if not result:
        raise Exception("Cannot parse table arn")

    target_database = result.group(3)
    target_table_name = result.group(4)
    table_name = target_table_name
    target_account_id = result.group(2)
    target_region = result.group(1)

    consumer_database = event['detail']['subscription']['glueConsumerDBName']
    user_role_arn = event['detail']['subscription']['athenaUserRoleArn']

    try:
        logger.info(f"Granting current principal to {consumer_database} Database to {lambda_principal}")
        grant_all_on_database(
            current_account_id,
            consumer_database,
            lambda_principal
        )

    except ClientError as e:
        logger.error(e)
        raise Exception(e)

    logger.info(
        f"Create resource Link {table_name} in database {consumer_database} targeting {target_database}.{target_table_name}")

    # check if we are in same account
    # if current_account_id == target_account_id:
    #     logger.info("Same account - no need to create resource link")
    # else:
    create_resource_link_table(consumer_database, table_name, target_database, target_table_name, target_account_id, target_region)
    # TODO if already created ?

    # First, grant Lambda principal to be able to grant to other principal
    grant_read_on_database(target_account_id, target_database, lambda_principal, allows_grants=True)
    grant_read_on_table(target_account_id, target_database, table_name, lambda_principal, allows_grants=True)

    distinct_deps_databases = set()
    distinct_deps_databases.add(target_database)

    for glueDep in event['detail']['glueDependencies']:
        # First, grant Lambda principal to be able to grant to other principal
        grant_read_on_database(target_account_id, glueDep['database_name'], lambda_principal, allows_grants=True)
        grant_read_on_table(target_account_id, glueDep['database_name'], glueDep['table_name'], lambda_principal,
                            allows_grants=True)

        distinct_deps_databases.add(glueDep['database_name'])
        grant_read_on_table(target_account_id, glueDep['database_name'], glueDep['table_name'], user_role_arn)

    for database_name in distinct_deps_databases:

        # check if we are in same account
        if current_account_id == target_account_id:
            logger.info("Same account - no need to create resource link")
        else:
            create_resource_link_database(database_name, database_name, target_account_id, target_region)

        # Grant on resource link (or localdatabase if we are in same account) target
        grant_read_on_database(target_account_id, database_name, user_role_arn)
        # Grant on resource link (or localdatabase if we are in same account)
        grant_read_on_database(current_account_id, database_name, user_role_arn)

    grant_read_on_table(target_account_id, target_database, table_name, user_role_arn)

    ensure_role_has_extended_policy(user_role_arn)


current_account_id = get_current_account_id()
lambda_principal = get_current_principal_identifier()


def lambda_handler(event, context):
    logger.info(event)

    if event['detail-type'] == 'Unmanaged Asset Subscription Request Accepted':
        handle_unmanaged_asset_subscription_on_producer(event)

    elif event['detail-type'] == 'Unmanaged Asset Successfully Granted in Pub Environment':
        handle_unmanaged_asset_subscription_on_consumer(event)
