import * as cdk from "aws-cdk-lib";
import {Construct} from "constructs";
import * as events from 'aws-cdk-lib/aws-events';
import * as eventsTargets from 'aws-cdk-lib/aws-events-targets';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambdas from "aws-cdk-lib/aws-lambda";
import * as lambda from "aws-cdk-lib/aws-lambda";
import {Layer} from "./layer";
import * as path from "path";
import * as logs from "aws-cdk-lib/aws-logs";
import * as cr from "aws-cdk-lib/custom-resources";
import {CustomResource} from "aws-cdk-lib";

export interface CustomDataZoneViewSubscriptionEnvironmentStackProps extends cdk.StackProps {
  datazone: {
    accountId: string,
    eventBusName: string,
    events: {
      source: string
    }
  },
  lambda: {
    architecture: lambda.Architecture,
    runtime: lambda.Runtime,
  }
}

export class CustomDataZoneViewSubscriptionEnvironmentStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: CustomDataZoneViewSubscriptionEnvironmentStackProps) {
    super(scope, id, props);

    // Create a new EventBus for Datazone
    const datazoneBus = new events.EventBus(this, 'DataZone', {
      eventBusName: props.datazone.eventBusName,
    });
    // Allows the Datazone domain, which is possibly on another account, to put events on the bus
    datazoneBus.addToResourcePolicy(new iam.PolicyStatement({
      sid: 'AllowPutEventsFromDatazoneContext',
      actions: ['events:PutEvents'],
      resources: [datazoneBus.eventBusArn],
      principals: [
        new iam.AccountPrincipal(this.account),
        new iam.AccountPrincipal(props.datazone.accountId)
      ]
    }));

    const customSubscriptionRule = new events.Rule(this, 'CustomSubscription', {
      eventBus: datazoneBus,
      eventPattern: {
        detailType: ['Subscription Request Accepted'],
        source: [props.datazone.events.source]
      }
    });

    const commonLayer = new Layer(this, "CommonLayer", {
      runtime: props.lambda.runtime,
      architecture: props.lambda.architecture,
      path: path.join(__dirname, '..', 'lambdas', "common-layer"),
    });

    const customSubscriptionFunction = new lambdas.Function(this, 'CustomSubscriptionFunction', {
      runtime: props.lambda.runtime,
      architecture: props.lambda.architecture,
      code: lambda.Code.fromAsset(path.join(__dirname, '..', 'lambdas', 'custom-subscription')),
      handler: 'index.lambda_handler',
      logRetention: logs.RetentionDays.TWO_WEEKS,
      layers: [
        commonLayer.layer
      ]
    });
    // Register the dispatch Lambda on the rule
    customSubscriptionRule.addTarget(new eventsTargets.LambdaFunction(customSubscriptionFunction));

    // Allows to retrieve Tables details
    customSubscriptionFunction.addToRolePolicy(
      new iam.PolicyStatement({
          actions: [
            'glue:CreateTable',
            'glue:GetTable',
          ],
          resources: [
            `arn:aws:glue:${this.region}:${this.account}:catalog`,
            `arn:aws:glue:${this.region}:${this.account}:database/*`,
            `arn:aws:glue:${this.region}:${this.account}:table/*/*`
          ]
        }
      ));

    // Allows to Grant lakeFormation permissions
    customSubscriptionFunction.addToRolePolicy(
      new iam.PolicyStatement({
          actions: [
            'lakeformation:GrantPermissions'
          ],
          resources: [
            `arn:aws:lakeformation:${this.region}:${this.account}:catalog:${this.account}`,
          ]
        }
      ));

    new cdk.CfnOutput(this, 'CustomSubscribeDatazoneRoleToBeAddedAsLakeFormationAdministrator', {
      value: customSubscriptionFunction.role!.roleArn
    });

    // Create a custom resource for handling DatalakeAdministrator

    // first create function
    const lakeformationDefaultSettingsHandler = new lambdas.Function(this, 'LakeformationDefaultSettingsHandler', {
      runtime: props.lambda.runtime,
      architecture: props.lambda.architecture,
      code: lambda.Code.fromAsset(path.join(__dirname, '..', 'lambdas', 'custom-resource-lf-admin')),
      handler: 'index.lambda_handler',
      logRetention: logs.RetentionDays.TWO_WEEKS,
      layers: [
        commonLayer.layer
      ],
      environment: {
        AWS_ACCOUNT: this.account,
        REGION: this.region
      }
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

    const lakeformationDefaultSettingsProvider = new cr.Provider(this,'LakeformationDefaultSettingsProvider', {
      onEventHandler: lakeformationDefaultSettingsHandler,
    });

    new CustomResource(this,'DefaultLakeFormationSettings', {
      serviceToken: lakeformationDefaultSettingsProvider.serviceToken,
      resourceType: 'Custom::LakeformationDefaultSettings',
      properties: {
        DataLakeAdmins: [
          customSubscriptionFunction.role?.roleArn,
        ]
      },
    });

  }
}