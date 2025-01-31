#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import {CustomDataZoneViewSubscriptionDomainStack} from '../lib/custom-data-zone-view-subscription-domain-stack';
import {
  CustomDataZoneViewSubscriptionEnvironmentStack
} from "../lib/custom-data-zone-view-subscription-environment-stack";
import {Environment} from "aws-cdk-lib/core/lib/environment";
import * as path from "node:path";


const app = new cdk.App();

const lambdaConfig= {
  runtime: lambda.Runtime.PYTHON_3_11,
  architecture: lambda.Architecture.ARM_64
}

const configName = app.node.tryGetContext("config") ?? "sandbox";
const config = require(path.join(__dirname, '..', 'config', `${configName}.config.json`));

// The DataZone Domain account
const datazoneDomainEnv = config.datazone.domainAwsAccount;
// The list of account that hosts DataZone environments
const targetEnvs: Environment[] = config.datazone.environmentsAwsAccounts;

// The name of the dedicated Event Bus to create in the target account
const targetEventBusName = 'datazone-custom-bus';
const targetEventSource = 'custom.datazone';
// The main stack in the domain account
const domainStack = new CustomDataZoneViewSubscriptionDomainStack(app, 'DatazoneCustomViewSubscriptionDomainStack', {
  datazone: {
    targetEventBusName: targetEventBusName,
    targetEventSource: targetEventSource
  },
  lambda: lambdaConfig,
  env: datazoneDomainEnv,
  debugEventBridge: true
});

// deployment
targetEnvs.forEach(env => {
  new CustomDataZoneViewSubscriptionEnvironmentStack(app, `DatazoneCustomViewSubscriptionEnvironmentStack-${env.account}-${env.region}`, {
    stackName: 'DatazoneCustomViewSubscriptionEnvironmentStack',
    env,
    datazone: {
      accountId: datazoneDomainEnv.account,
      eventBusName: targetEventBusName,
      events: {
        source: targetEventSource
      },
      environmentAccounts:  targetEnvs
    },
    lambdaRuntime: lambdaConfig,
    debugEventBridge: true
  });
});

