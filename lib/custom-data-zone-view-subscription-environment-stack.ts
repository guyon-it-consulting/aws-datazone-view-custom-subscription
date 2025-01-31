import * as cdk from "aws-cdk-lib";
import {Construct} from "constructs";
import * as events from 'aws-cdk-lib/aws-events';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as eventsTargets from 'aws-cdk-lib/aws-events-targets';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambdas from "aws-cdk-lib/aws-lambda";
import * as lambda from "aws-cdk-lib/aws-lambda";
import {Layer} from "./layer";
import * as path from "path";
import * as logs from "aws-cdk-lib/aws-logs";
import {DebugEventBridge} from "./debug-event-bridge";
import {SetUpAdditionalLakeFormationAdministrator} from "./set-up-additional-lake-formation-administrator";

export interface CustomDataZoneViewSubscriptionEnvironmentStackProps extends cdk.StackProps {
  datazone: {
    accountId: string,
    eventBusName: string,
    events: {
      source: string
    }
    environmentAccounts: cdk.Environment[]
  },
  lambdaRuntime: {
    architecture: lambda.Architecture,
    runtime: lambda.Runtime,
  }
  debugEventBridge?: boolean
}

export class CustomDataZoneViewSubscriptionEnvironmentStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: CustomDataZoneViewSubscriptionEnvironmentStackProps) {
    super(scope, id, props);

    // Create a new EventBus for Datazone
    const datazoneBus = new events.EventBus(this, 'DataZone', {
      eventBusName: props.datazone.eventBusName,
    });

    const principalAccounts = [
      this.account,
      props.datazone.accountId,
      ...props.datazone.environmentAccounts.map(env => env.account)
    ];

    // Allows the Datazone domain, which is possibly on another account, to put events on the bus
    datazoneBus.addToResourcePolicy(new iam.PolicyStatement({
      sid: 'AllowPutEventsFromDatazoneContext',
      actions: ['events:PutEvents'],
      resources: [datazoneBus.eventBusArn],
      principals: [...new Set(principalAccounts.map(account => new iam.AccountPrincipal(account)))],
    }));

    const customSubscriptionRule = new events.Rule(this, 'CustomSubscription', {
      eventBus: datazoneBus,
      eventPattern: {
        detailType: [
          'Unmanaged Asset Subscription Request Accepted',
          'Unmanaged Asset Successfully Granted in Pub Environment'
        ],
        source: [props.datazone.events.source]
      }
    });


    // create custom Managed Policy for datazone_usr
    const datazoneCustomGrant = new iam.ManagedPolicy(this, 'DataZoneUserRoleCrossAccountGlueCatalogReadAccess', {
      managedPolicyName: 'DataZoneUserRoleCrossAccountGlueCatalogReadAccess',
      document: new iam.PolicyDocument({
        statements: [
          new iam.PolicyStatement({
            actions: [
              'glue:GetDatabase',
              'glue:GetDatabases',
              'glue:GetTables',
              'glue:GetPartition',
              'glue:BatchGetPartition',
            ],
            resources: ['*']
          })
        ]
      })
    });

    // Create DynamoDB Table
    const subscriptionTable = new dynamodb.TableV2(this, 'AssetsPerPrincipalSubscriptionsTable', {
      partitionKey: {name: 'principalArn', type: dynamodb.AttributeType.STRING},
      sortKey: {name: 'targetGlueAsset', type: dynamodb.AttributeType.STRING},
      pointInTimeRecovery: true
    });


    const commonLayer = new Layer(this, "CommonLayer", {
      runtime: props.lambdaRuntime.runtime,
      architecture: props.lambdaRuntime.architecture,
      path: path.join(__dirname, '..', 'lambdas', "common-layer"),
    });

    const customSubscriptionFunction = new lambdas.Function(this, 'CustomSubscriptionFunction', {
      description: 'Handle Datazone Unmanaged Assets',
      runtime: props.lambdaRuntime.runtime,
      architecture: props.lambdaRuntime.architecture,
      code: lambda.Code.fromAsset(path.join(__dirname, '..', 'lambdas', 'custom-subscription')),
      handler: 'index.lambda_handler',
      logRetention: logs.RetentionDays.TWO_WEEKS,
      layers: [
        commonLayer.layer,
        lambda.LayerVersion.fromLayerVersionArn(this, 'PowerTools', `arn:aws:lambda:${cdk.Stack.of(this).region}:017000801446:layer:AWSLambdaPowertoolsPythonV2-Arm64:79`)
      ],
      timeout: cdk.Duration.minutes(1),
      environment: {
        EVENT_BUS_NAME: props.datazone.eventBusName,
        EVENT_SOURCE: props.datazone.events.source,
        DATAZONE_USER_CUSTOM_MANAGED_POLICY_ARN: datazoneCustomGrant.managedPolicyArn,
        SUBSCRIPTION_TABLE_NAME: subscriptionTable.tableName
      }
    });
    // Register the dispatch Lambda on the rule
    customSubscriptionRule.addTarget(new eventsTargets.LambdaFunction(customSubscriptionFunction));

    subscriptionTable.grantReadWriteData(customSubscriptionFunction);

    // Allows to retrieve Tables details
    customSubscriptionFunction.addToRolePolicy(
      new iam.PolicyStatement({
          actions: [
            'glue:GetTable',
            'glue:GetTables',
            'glue:GetDatabase',
          ],
          resources: [
            // `arn:aws:glue:${this.region}:${this.account}:catalog`,
            // `arn:aws:glue:${this.region}:${this.account}:database/*`,
            // `arn:aws:glue:${this.region}:${this.account}:table/*/*`
            // it needs to access cross account/region resources
            `arn:aws:glue:*:*:catalog`,
            `arn:aws:glue:*:*:database/*`,
            `arn:aws:glue:*:*:table/*/*`
          ]
        }
      ));

    // Allows to create Database Tables details
    customSubscriptionFunction.addToRolePolicy(
      new iam.PolicyStatement({
          actions: [
            'glue:CreateTable',
            'glue:CreateDatabase',
          ],
          resources: [
            `arn:aws:glue:${this.region}:${this.account}:catalog`,
            `arn:aws:glue:${this.region}:${this.account}:database/*`,
            `arn:aws:glue:${this.region}:${this.account}:table/*/*`
          ]
        }
      ));

    // Allows to manage Datazone user role inline policy
    customSubscriptionFunction.addToRolePolicy(
      new iam.PolicyStatement({
          actions: [
            'iam:ListAttachedRolePolicies',
            'iam:AttachRolePolicy',
          ],
          resources: [
            `arn:aws:iam::${this.account}:role/datazone_usr_*`,
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

    customSubscriptionFunction.addToRolePolicy(
      new iam.PolicyStatement({
          actions: [
            'ram:CreateResourceShare',
          ],
          resources: [
            '*'
          ],
          conditions: {
            // string like if exists
            'StringLikeIfExists': {
              'ram:RequestedResourceType': [
                'glue:Table',
                'glue:Database',
                'glue:Catalog'
              ]
            }
          }
        }
      ));

    customSubscriptionFunction.addToRolePolicy(
      new iam.PolicyStatement({
          actions: [
            'ram:UpdateResourceShare',
            'ram:DeleteResourceShare',
            'ram:AssociateResourceShare',
            'ram:DisassociateResourceShare',
            'ram:GetResourceShares',
          ],
          resources: [
            '*'
          ],
          conditions: {
            'StringLike': {
              'ram:ResourceShareName': [
                'LakeFormation*'
              ]
            }
          }
        }
      ));

    customSubscriptionFunction.addToRolePolicy(
      new iam.PolicyStatement({
          actions: [
            'glue:PutResourcePolicy',
            'glue:DeleteResourcePolicy',
            'organizations:DescribeOrganization',
            'organizations:DescribeAccount',
            'ram:Get*',
            'ram:List*',
            'organizations:ListRoots',
            'organizations:ListAccountsForParent',
            'organizations:ListOrganizationalUnitsForParent'
          ],
          resources: [
            '*'
          ]
        }
      ));

    customSubscriptionFunction.addToRolePolicy(
      new iam.PolicyStatement({
          actions: [
            'glue:PutResourcePolicy',
            'glue:DeleteResourcePolicy',
            'organizations:DescribeOrganization',
            'organizations:DescribeAccount',
          ],
          resources: [
            '*'
          ]
        }
      ));

    customSubscriptionFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: [
          'iam:GetRole'
        ],
        resources: ['*']
      })
    );

    // Allow the Lambda to put events on the target event bus, on all region/account
    customSubscriptionFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ['events:PutEvents'],
        resources: [`arn:aws:events:*:*:event-bus/${props.datazone.eventBusName}`],
      })
    );

    new cdk.CfnOutput(this, 'CustomSubscribeDatazoneRoleToBeAddedAsLakeFormationAdministrator', {
      value: customSubscriptionFunction.role!.roleArn
    });

    // Add the function as a DatalakeAdministrator
    new SetUpAdditionalLakeFormationAdministrator(this, 'LakeFormationAdministrator', {
      lambda: props.lambdaRuntime,
      rolesArn: [
        customSubscriptionFunction.role!.roleArn
      ],
    });

    if (props.debugEventBridge ?? false) {
      new DebugEventBridge(this, `DebugProducerEventBridge`, {
          eventBusName: props.datazone.eventBusName,
          eventPattern: {
            source: [props.datazone.events.source],
          },
          logGroupPrefix: 'debug-producer'
      });
    }
  }
}


