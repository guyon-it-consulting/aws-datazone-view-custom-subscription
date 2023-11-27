import json
import logging
import re
import boto3
import base64
from sql_metadata import Parser
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sts_client = boto3.client('sts')
iam_client = boto3.client('iam')
glue_client = boto3.client('glue')
lf_client = boto3.client('lakeformation')
datazone_client = boto3.client('datazone')

def get_current_principal_identifier():
    callerArn = sts_client.get_caller_identity()['Arn']
    logger.info(f"callerArn: {callerArn}")

    import re
    match = re.search(r'^arn:aws:sts::(\d+):assumed-role/([\w-]+)/([\w-]+)$', callerArn)
    if match:
        roleName = match.group(2)
        roleArn = iam_client.get_role(RoleName=roleName)['Role']['Arn']
        return roleArn
    else:
        # TODO do better
        return ""

def grant_database(database_name, principal_arn):
    logger.info(f"granting {database_name} to {principal_arn}")

    try:
        lf_client.grant_permissions(
            Principal={
                'DataLakePrincipalIdentifier': principal_arn
            },
            Resource={
                'Database': {
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

# Grant Read on a given table
def grant_table(database_name, table_name, principal_arn):
    logger.info(f"granting {database_name}.{table_name} to {principal_arn}")

    try:
        lf_client.grant_permissions(
            Principal={
                'DataLakePrincipalIdentifier': principal_arn
            },
            Resource={
                'Table': {
                    'DatabaseName': database_name,
                    'Name': table_name
                }
            },
            Permissions=[
                'SELECT',
                'DESCRIBE',
            ]
        )
    except ClientError as e:
        logger.error(e)
        raise Exception(e)


# Cache of glue:GetTable responses, aim to speed-up the analyzeview process
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

    view_orignal_text = resp['Table']['ViewOriginalText']

    base64_regex = "(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?"

    result = re.search(f"^/\*\sPresto\sView:\s({base64_regex})\s\*/$", view_orignal_text)
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


def lambda_handler(event, context):
    logger.info(event['detail-type'])

    if event['detail-type'] == 'Subscription Request Accepted':
        logger.info('Hande subscription')

        if event['detail']['data']['isManagedAsset']:
            logger.info("This is a managed asset - ignore it")
            return

        if event['detail']['listing']:
            forms = json.loads(event['detail']['listing']['forms'])

            table_arn = forms['GlueViewForm']['tableArn']
            query = forms['GlueViewForm']['query']

            result = re.search("^arn:aws:glue:(\w+-\w+-\d):(\d+):table/(\w+)/(\w+)$", table_arn)
            if not result:
                raise Exception("Cannot parse table arn")

            database = result.group(3)
            table_name = result.group(4)
            account_id = result.group(2)
            region = result.group(1)

            logger.info(f"Handle table {table_name} in Database: {database}")

            consumer_database = next(filter(lambda res: res['name'] == 'glueConsumerDBName',
                                            event['detail']['environment']['provisionedResources']), None)['value']
            user_role_arn = next(filter(lambda res: res['name'] == 'userRoleArn',
                                        event['detail']['environment']['provisionedResources']), None)['value']

            try:
                lambda_principal = get_current_principal_identifier()
                logger.info(f"Granting current principal to {consumer_database} Database to {lambda_principal}")
                grant_database(
                    database_name=consumer_database,
                    principal_arn=lambda_principal
                )

                logger.info(
                    f"Create resource Link {table_name} in database {consumer_database} targeting {database}.{table_name}")

                glue_client.create_table(
                    DatabaseName=consumer_database,
                    TableInput={
                        'Name': table_name,
                        'TargetTable': {
                            'DatabaseName': database,
                            'Name': table_name,
                            'CatalogId': account_id,
                            'Region': region
                        }
                    }
                )
                logger.info("Resource Link created")

            except ClientError as e:
                logger.error(e)
                raise Exception(e)

            # not Needed because ALL_TABLES SELECT/DESRIBE grant by default in DataZone provisionning
            # # Grant this resource link to datazone user
            # grant_table(consumer_database, table_name, user_role_arn)

            # Grant underlying view to datazone user
            grant_table(database, table_name, user_role_arn)

            # Analyze view, and find dependents views & tables that needs to be granted to datazone user
            for dependent_item in analyze_view(database, table_name):
                logger.info(f"Dependent resource found {dependent_item}")
                dependent_item_split = dependent_item.split('.')
                # grant this dependent to datazone user
                grant_table(dependent_item_split[0], dependent_item_split[1], user_role_arn)
