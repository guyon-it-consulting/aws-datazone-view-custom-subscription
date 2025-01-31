import os
import boto3
from botocore.exceptions import ClientError

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.data_classes import event_source, CloudFormationCustomResourceEvent

logger = Logger()

AWS_ACCOUNT = os.environ.get('AWS_ACCOUNT')
AWS_REGION = os.environ.get('AWS_REGION')
lf_client = boto3.client("lakeformation", region_name=AWS_REGION)
iam_client = boto3.client('iam')


def clean_props(**props):
    data = {k: props[k] for k in props.keys() if k != 'ServiceToken'}
    return data


def validate_principals(principals):
    validated_principals = []
    for principal in principals:
        if ":role/" in principal:
            logger.info(f'Principal {principal} is an IAM role, validating....')
            try:
                iam_client.get_role(RoleName=principal.split("/")[-1])
                logger.info(f'Adding principal {principal} to validated principals')
                validated_principals.append(principal)
            except Exception as e:
                logger.exception(f'Failed to get role {principal} due to: {str(e)}')
    return validated_principals

@logger.inject_lambda_context(log_event=True)
@event_source(data_class=CloudFormationCustomResourceEvent)
def lambda_handler(event: CloudFormationCustomResourceEvent, context: LambdaContext):
    request_type = event.request_type
    if request_type == 'Create':
        return on_create(event)
    if request_type == 'Update':
        return on_update(event)
    if request_type == 'Delete':
        return on_delete(event)
    raise Exception('Invalid request type: %s' % request_type)


def on_create(event: CloudFormationCustomResourceEvent):
    """"Adds the PivotRole to the existing Data Lake Administrators
    Before adding any principal, it validates it exists if it is an IAM role
    """
    props = clean_props(**event.resource_properties)
    try:
        response = lf_client.get_data_lake_settings(CatalogId=AWS_ACCOUNT)
        existing_admins = response.get('DataLakeSettings', {}).get('DataLakeAdmins', [])
        if existing_admins:
            existing_admins = [
                admin['DataLakePrincipalIdentifier'] for admin in existing_admins
            ]

        new_admins = props.get('DataLakeAdmins', [])
        new_admins.extend(existing_admins or [])
        validated_new_admins = validate_principals(new_admins)

        response = lf_client.put_data_lake_settings(
            CatalogId=AWS_ACCOUNT,
            DataLakeSettings={
                'DataLakeAdmins': [
                    {'DataLakePrincipalIdentifier': principal}
                    for principal in validated_new_admins
                ]
            },
        )
        logger.info(f'Successfully configured AWS LakeFormation data lake admins: {validated_new_admins}| {response}')

    except ClientError as e:
        logger.exception(f'Failed to setup AWS LakeFormation data lake admins due to: {e}')
        raise Exception(f'Failed to setup AWS LakeFormation data lake admins due to: {e}')

    return {
        'PhysicalResourceId': f'LakeFormationDefaultSettings{AWS_ACCOUNT}{AWS_REGION}'
    }


def on_update(event: CloudFormationCustomResourceEvent):
    return on_create(event)


def on_delete(event: CloudFormationCustomResourceEvent):
    """"Removes the PivotRole from the existing Data Lake Administrators
    Before adding any principal, it validates it exists if it is an IAM role
    """
    props = clean_props(**event.resource_properties)
    try:
        response = lf_client.get_data_lake_settings(CatalogId=AWS_ACCOUNT)
        existing_admins = response.get('DataLakeSettings', {}).get('DataLakeAdmins', [])
        if existing_admins:
            existing_admins = [
                admin['DataLakePrincipalIdentifier'] for admin in existing_admins
            ]

        added_admins = props.get('DataLakeAdmins', [])
        for added_admin in added_admins:
            if added_admin in existing_admins:
                existing_admins.remove(added_admin)

        validated_new_admins = validate_principals(existing_admins)
        response = lf_client.put_data_lake_settings(
            CatalogId=AWS_ACCOUNT,
            DataLakeSettings={
                'DataLakeAdmins': [
                    {'DataLakePrincipalIdentifier': principal}
                    for principal in validated_new_admins
                ]
            },
        )
        logger.info(f'Successfully configured AWS LakeFormation data lake admins: {validated_new_admins}| {response}')

    except ClientError as e:
        logger.exception(f'Failed to setup AWS LakeFormation data lake admins due to: {e}')
        raise Exception(f'Failed to setup AWS LakeFormation data lake admins due to: {e}')

    return {
        'PhysicalResourceId': f'LakeFormationDefaultSettings{AWS_ACCOUNT}{AWS_REGION}'
    }
