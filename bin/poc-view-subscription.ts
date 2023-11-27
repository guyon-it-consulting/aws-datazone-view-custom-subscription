#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import {CustomDataZoneViewSubscriptionDomainStack} from '../lib/custom-data-zone-view-subscription-domain-stack';
import {
  CustomDataZoneViewSubscriptionEnvironmentStack
} from "../lib/custom-data-zone-view-subscription-environment-stack";
import {Environment} from "aws-cdk-lib/core/lib/environment";
import {DebugEventBridgeStack} from "../lib/debug-event-bridge-stack";


const app = new cdk.App();

const lambdaConfig= {
  runtime: lambda.Runtime.PYTHON_3_11,
  architecture: lambda.Architecture.ARM_64
}

// The DataZone Domain account
const datazoneDomainEnv = {account: 'YOUR_DATAZONE_DOMAIN_AWS_ACCOUNT_ID', region: 'YOUR_DATAZONE_DOMAIN_AWS_REGION'};
// The list of account that hosts DataZone environments
const targetEnvs: Environment[] = [
  {account: 'YOUR_DATAZONE_ENVIRONMENT_AWS_ACCOUNT_ID', region: 'YOUR_DATAZONE_ENVIRONMENT_AWS_REGION'}
];

// The name of the dedicated Event Bus to create in the target account
const targetEventBusName = 'datazone-custom-bus';
const targetEventSource = 'custom.datazone';
// The main stack in the domain account
const domainStack = new CustomDataZoneViewSubscriptionDomainStack(app, 'PocViewSubscriptionStack', {
  datazone: {
    domainId: 'dzd_apcwv3stgpbj4m',
    targetEventBusName: targetEventBusName,
    targetEventSource: targetEventSource
  },
  lambda: lambdaConfig,
  env: datazoneDomainEnv,
});
new DebugEventBridgeStack(app, 'DebugDomainEventBridgeStack', {
  env: datazoneDomainEnv,
  debug: {
    eventBusName: 'default',
    eventPattern: {
      source: ['aws.datazone'],
    },
    logGroupPrefix: 'debug-domain'
  }
})

function formatResourceName(s: string) {
  return replaceAll(replaceAll(s,'-',''), '_', '');
}

function replaceAll(str: string, search: string, replacement: string) {
  return str.split(search).join(replacement);
}

// deployment
targetEnvs.forEach(env => {
  new CustomDataZoneViewSubscriptionEnvironmentStack(app, formatResourceName(`DatazoneProducerAccountStack_${env.account}_${env.region}`), {
    stackName: 'DatazoneProducerAccountStack',
    env,
    datazone: {
      accountId: datazoneDomainEnv.account,
      eventBusName: targetEventBusName,
      events: {
        source: targetEventSource
      },
      environmentAccounts:  targetEnvs
    },
    lambda: lambdaConfig,
  });

  new DebugEventBridgeStack(app, formatResourceName(`DebugProducerEventBridgeStack_${env.account}_${env.region}`), {
    env,
    debug: {
      eventBusName: targetEventBusName,
      eventPattern: {
        source: [targetEventSource],
      },
      logGroupPrefix: 'debug-producer'
    }
  })
});

