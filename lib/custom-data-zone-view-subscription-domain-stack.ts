import * as cdk from 'aws-cdk-lib';
import {Construct} from 'constructs';
import * as events from 'aws-cdk-lib/aws-events';
import * as eventsTargets from 'aws-cdk-lib/aws-events-targets';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as path from "path";
import * as logs from "aws-cdk-lib/aws-logs";
import * as iam from "aws-cdk-lib/aws-iam";
import {Layer} from "./layer";

export interface CustomDataZoneViewSubscriptionDomainStackProps extends cdk.StackProps {
  datazone: {
    targetEventBusName?: string,
    targetEventSource: string
  },
  lambda: {
    architecture: lambda.Architecture,
    runtime: lambda.Runtime,
  }
}

export class CustomDataZoneViewSubscriptionDomainStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: CustomDataZoneViewSubscriptionDomainStackProps) {
    super(scope, id, props);

    // Create a new Rule that listen to detailType "Subscription Grant Completed" on source "aws.datazone"
    const subGrantCompleteRule = new events.Rule(this, 'SubscriptionGrantCompletedRule', {
      eventPattern: {
        detailType: ['Subscription Request Accepted'],
        source: ['aws.datazone'],
      }
    });

    const commonLayer = new Layer(this, "CommonLayer", {
      runtime: props.lambda.runtime,
      architecture: props.lambda.architecture,
      path: path.join(__dirname, '..', 'lambdas', "common-layer"),
    });

    const targetEventBusName = props.datazone.targetEventBusName ?? 'default';

    // the target EventBus depends on the content of the event itself.
    // So let's use a Lambda in order to propagate the event to the right account.
    const dispatchEventFunction = new lambda.Function(this, 'DispatchDatazoneEvents', {
      runtime: props.lambda.runtime,
      architecture: props.lambda.architecture,
      code: lambda.Code.fromAsset(path.join(__dirname, '..', 'lambdas', 'event-dispatcher')),
      handler: 'index.lambda_handler',
      logRetention: logs.RetentionDays.TWO_WEEKS,
      layers: [
        commonLayer.layer
      ],
      environment: {
        EVENT_BUS_NAME: targetEventBusName,
        EVENT_SOURCE: props.datazone.targetEventSource
      },
    });

    // Register the dispatch Lambda on the rule
    subGrantCompleteRule.addTarget(new eventsTargets.LambdaFunction(dispatchEventFunction));

    // Allow the Lambda to get Info from the Datazone domain
    dispatchEventFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: [
          'datazone:GetListing',
          'datazone:GetEnvironment',
          'datazone:ListEnvironments',
        ],
        // resources: [`arn:aws:datazone:${this.region}:${this.account}:domain/${props.datazone.domainId}`],
        resources: [`arn:aws:datazone:${this.region}:${this.account}:domain/*`],
      })
    );

    // Allow the Lambda to put events on the target event bus, on all region/account
    dispatchEventFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ['events:PutEvents'],
        resources: [`arn:aws:events:*:*:event-bus/${targetEventBusName}`],
      })
    );
  }
}


