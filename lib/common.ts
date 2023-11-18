import * as cdk from "aws-cdk-lib";

export const COMMON_BUNDLING_OPTIONS = {
  ...(process.env.DOCKER_VOLUME_COPY === "1" && {bundlingFileAccess: cdk.BundlingFileAccess.VOLUME_COPY}),
};