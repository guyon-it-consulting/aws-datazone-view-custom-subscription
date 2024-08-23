import * as cdk from "aws-cdk-lib";
import {Construct} from "constructs";
import * as events from "aws-cdk-lib/aws-events";
import * as events_targets from "aws-cdk-lib/aws-events-targets";
import * as logs from "aws-cdk-lib/aws-logs";

export interface DebugEventBridgeProps {
  eventBusName?: string,
  eventPattern: cdk.aws_events.EventPattern,
  logGroupRetention?: logs.RetentionDays,
  logGroupPrefix?: string
}

export class DebugEventBridge extends Construct {
  constructor(scope: Construct, id: string, props: DebugEventBridgeProps) {
    super(scope, id);

    const eventBusName = props.eventBusName ?? 'default';
    const rule = new events.Rule(this, 'DebugRule', {
      eventBus: events.EventBus.fromEventBusName(this, 'EventBus', eventBusName),
      eventPattern: props.eventPattern
    });

    const logGroup = new logs.LogGroup(this, 'DebugLogGroup', {
      logGroupName: `/aws/events/${props.logGroupPrefix ?? 'debug'}-${eventBusName}`,
      retention: props.logGroupRetention ?? logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    rule.addTarget(new events_targets.CloudWatchLogGroup(logGroup));

    //create an output of the logroup
    new cdk.CfnOutput(this, 'DebugLogGroupOutput', {
      value: logGroup.logGroupArn,
      description: 'The ARN of the debug log group'
    });
  }
}