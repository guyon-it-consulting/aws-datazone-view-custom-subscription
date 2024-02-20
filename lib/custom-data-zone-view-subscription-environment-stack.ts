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
    environmentAccounts: cdk.Environment[]
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
      ],
      timeout: cdk.Duration.minutes(1),
      environment: {
        EVENT_BUS_NAME: props.datazone.eventBusName,
        EVENT_SOURCE: props.datazone.events.source,
        DATAZONE_USER_CUSTOM_MANAGED_POLICY_ARN: datazoneCustomGrant.managedPolicyArn
      }
    });
    // Register the dispatch Lambda on the rule
    customSubscriptionRule.addTarget(new eventsTargets.LambdaFunction(customSubscriptionFunction));

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