# Datazone View Subscription Proof of concept

This project is a proof of concept on how provide access to Datazone unmanaged assets, with cross account constraints
This is targeted on granting access to Glue View trough Athena.

It handles cascading dependencies between views.

Let's says we have got :
* Database D1 :
  * Table T1
  * View V1 : `SELECT * FROM D1.T1`
* Database D2 :
  * View V2 : `SELECT * FROM D1.V1`

If we want to subscribe to `D2.V2` :
* The process will analyze `D2.V2`
  * Find deps on `D1.V1`, `D1.T1`
* Create a resource link of `D2.V2` in sub_db database
* Grant access to Datazone principal on `D2.V2`, `D1.V1`, and `D1.T1`

So Subscribed asset `D2.V2` appears in the Consumer Database `_sub_db` is queryable.

Here the architecture and process.

![](doc/datazone.drawio.png)

## Steps

- Create a Datazone domain, a project in account A
- configure and deploy a Datazone environment in account B
- in account B resides Glue Database and Glue Tables/Views (prerequisite or to be created manually)
- Edit `./bin/poc-view-subscription.ts`
  - replace `YOUR_DOMAIN_ID` by the Datazone domain of account A
  - replace `YOUR_DATAZONE_DOMAIN_AWS_ACCOUNT_ID` by the Aws Account id of the Datazone domain (account A)
  - replace `YOUR_DATAZONE_DOMAIN_AWS_REGION` by the Region of the Datazone domain
  - replace `YOUR_DATAZONE_ENVIRONMENT_AWS_ACCOUNT_ID` by the Aws Account id of the Datazone environment (account B), where your data resides
  - replace `YOUR_DATAZONE_ENVIRONMENT_AWS_REGION` by the Region of the Datazone Environment domain, where your data resides in account B
- Bootstrap CDK in accounts (see bellow)
- Deploy with `cdk deploy --all`


## Bootstrap the account for use with CDK

> You need to bootstrap all accounts where you want to deploy resources  with CDK

![](doc/cdk.drawio.png)

### Bootstrap the CICD Account

```bash
cdk bootstrap
```

### Bootstrap the Target Accounts

pre-requisite :
* you are a Lakeformation Administrator
* jq is installed
* `export AWS_PROFILE=xxxx` for the target account to CDK bootsrap

following script:
* keep existing trust on previously CDK bootstrap & trusted environments
* Add CDK deploy role from boostraped environment as a LakeFormation Administrator
  * To be allowed to grant LakeFormation capabilities from CDK

```bash
CDK_DEPLOYMENT_ACCOUNT=xyz

# Checking first if CDK has already been bootstrapped here and eventually add the deploy account in the trust
CURRENT_TRUSTED_ACCOUNTS=$(aws cloudformation describe-stacks --stack-name CDKToolkit --query 'Stacks[0].Parameters[?ParameterKey==`TrustedAccounts`].ParameterValue' --output text)
echo $CURRENT_TRUSTED_ACCOUNTS

if [[ "$CURRENT_TRUSTED_ACCOUNTS" == *"$CDK_DEPLOYMENT_ACCOUNT"* ]]
then
  echo "account already in the trust list"
  TRUSTED_ACCOUNTS=$CURRENT_TRUSTED_ACCOUNTS
else
  echo "not in the trust list add it"
  TRUSTED_ACCOUNTS=${CDK_DEPLOYMENT_ACCOUNT}${CURRENT_TRUSTED_ACCOUNTS:+,$CURRENT_TRUSTED_ACCOUNTS}
fi

CURRENT_REGION=$(aws configure get region)
CURRENT_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
cdk bootstrap --trust $TRUSTED_ACCOUNTS --cloudformation-execution-policies arn:aws:iam::aws:policy/AdministratorAccess aws://${CURRENT_ACCOUNT}/${CURRENT_REGION}

# add cdk exec role as LakeFormation administrator
CDK_QUALIFIER=$(aws cloudformation describe-stacks --stack-name CDKToolkit --query 'Stacks[0].Parameters[?ParameterKey==`Qualifier`].ParameterValue' --output text)

CDK_ROLE_TO_GRANT=cdk-${CDK_QUALIFIER}-cfn-exec-role-${CURRENT_ACCOUNT}-${CURRENT_REGION}
# check role existence 
CDK_ROLE_ARN_TO_GRANT=$(aws iam get-role --role-name $CDK_ROLE_TO_GRANT --query 'Role.Arn' --output text)

# assign as DatalakeAdministrator

EXISTING=$(aws lakeformation get-data-lake-settings --query "DataLakeSettings.DataLakeAdmins[?DataLakePrincipalIdentifier=='"$CDK_ROLE_ARN_TO_GRANT"'].DataLakePrincipalIdentifier" --output text)
if [ -z "$EXISTING" ];
then
      echo "$CDK_ROLE_ARN_TO_GRANT is not yet a lakeformation administrator, let's assign it."
      
      # Following retrieve current Lf settings, add the Role_arn into the array of DataLakeAdmins, then push it to Lf
      aws lakeformation get-data-lake-settings --output json  \
          | jq ".DataLakeSettings.DataLakeAdmins += [{\"DataLakePrincipalIdentifier\": \""$CDK_ROLE_ARN_TO_GRANT"\" }]" \
          | xargs -0 aws lakeformation put-data-lake-settings --cli-input-json
          
      echo "done"
else
      echo "$CDK_ROLE_ARN_TO_GRANT is already a lakeformation administrator, do nothing."
fi
```

## Useful commands

* `npm run build`   compile typescript to js
* `npm run watch`   watch for changes and compile
* `npm run test`    perform the jest unit tests
* `cdk deploy`      deploy this stack to your default AWS account/region
* `cdk diff`        compare deployed stack with current state
* `cdk synth`       emits the synthesized CloudFormation template

## Backlog

- [ ] Simplify the dispatched events
- [ ] Publish the subscription state back to the Datazone domain
- [ ] Support un-publish by removing grants on related resources
  - [ ] this requires a full analysis on all other subscribed Views!
- [ ] Provide more accurate SQL dependencies, on special SQL statements (UNNEST, ...)
  - [ ] try a Presto parser
  - [ ] try a bedrock analysis with Claude2
- [ ] analyze how to add guardrails on lakeformation grants
- [x] Add CustomResource to add the environment stack lambdarole as a LakeFormationAdministrator
- [ ] Use only default bus
  - [ ] Is that a good idea to add a resource policy on the default bus?
- [ ] Prepare a manual procedure to remove Subscription item
  - [ ] Remove ResourceLink
  - [ ] Revoke Grants on datazone _usr