import {Construct} from "constructs";
import * as lambdas from "aws-cdk-lib/aws-lambda";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as path from "node:path";
import * as logs from "aws-cdk-lib/aws-logs";
import * as cdk from "aws-cdk-lib";
import {CustomResource} from "aws-cdk-lib";
import * as iam from "aws-cdk-lib/aws-iam";
import * as cr from "aws-cdk-lib/custom-resources";

interface SetUpAdditionalLakeFormationAdministratorStackProps extends cdk.StackProps {
  lambda: {
    architecture: lambda.Architecture,
    runtime: lambda.Runtime,
  },
  rolesArn: string[]
}

export class SetUpAdditionalLakeFormationAdministrator extends Construct {
  constructor(scope: Construct, id: string, props: SetUpAdditionalLakeFormationAdministratorStackProps) {
    super(scope, id);

    // first create function
    const lakeformationDefaultSettingsHandler = new lambdas.Function(this, 'LakeformationDefaultSettingsHandler', {
      runtime: props.lambda.runtime,
      architecture: props.lambda.architecture,
      layers:[
        lambda.LayerVersion.fromLayerVersionArn(this, 'PowerTools', `arn:aws:lambda:${cdk.Stack.of(this).region}:017000801446:layer:AWSLambdaPowertoolsPythonV2-Arm64:79`)
      ],
      code: lambda.Code.fromAsset(path.join(__dirname, '..', 'lambdas', 'custom-resource-lf-admin')),
      handler: 'index.lambda_handler',
      logRetention: logs.RetentionDays.TWO_WEEKS,
      environment: {
        AWS_ACCOUNT: cdk.Stack.of(this).account,
        REGION: cdk.Stack.of(this).region
      },
    });

    lakeformationDefaultSettingsHandler.addToRolePolicy(
      new iam.PolicyStatement({
          actions: [
            'lakeformation:PutDataLakeSettings',
            'lakeformation:GetDataLakeSettings'
          ],
          resources: ['*']
        }
      ));
    lakeformationDefaultSettingsHandler.addToRolePolicy(
      new iam.PolicyStatement({
          actions: [
            'iam:GetRole',
          ],
          resources: ['*']
        }
      ));

    const lakeformationDefaultSettingsProvider = new cr.Provider(this, 'LakeformationDefaultSettingsProvider', {
      onEventHandler: lakeformationDefaultSettingsHandler,
    });

    new CustomResource(this, 'DefaultLakeFormationSettings', {
      serviceToken: lakeformationDefaultSettingsProvider.serviceToken,
      resourceType: 'Custom::LakeformationDefaultSettings',
      properties: {
        DataLakeAdmins: props.rolesArn
      },
    });
  }
}