import * as cdk from "aws-cdk-lib";
import {Construct} from "constructs";
import * as events from "aws-cdk-lib/aws-events";
import * as events_targets from "aws-cdk-lib/aws-events-targets";
import * as logs from "aws-cdk-lib/aws-logs";

export interface DebugEventBridgeStackProps extends cdk.StackProps {
  debug: {
    eventBusName?: string,
    eventPattern: cdk.aws_events.EventPattern,
    logGroupRetention?: logs.RetentionDays
  }
}

export class DebugEventBridgeStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: DebugEventBridgeStackProps) {
    super(scope, id, props);

    const eventBusName = props.debug.eventBusName ?? 'default';
    const rule = new events.Rule(this, 'DebugRule', {
      eventBus: events.EventBus.fromEventBusName(this, 'EventBus', eventBusName),
      eventPattern: props.debug.eventPattern
    });

    const logGroup = new logs.LogGroup(this, 'DebugLogGroup', {
      logGroupName: `/aws/events/debug-${eventBusName}`,
      retention: props.debug.logGroupRetention ?? logs.RetentionDays.ONE_WEEK
    });
    rule.addTarget(new events_targets.CloudWatchLogGroup(logGroup));

    //create an output of the logroup
    new cdk.CfnOutput(this, 'DebugLogGroupOutput', {
      value: logGroup.logGroupArn,
      description: 'The ARN of the debug log group'
    });
  }
}