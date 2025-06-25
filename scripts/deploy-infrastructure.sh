#!/bin/bash

ENVIRONMENT=${1:-dev}
STACK_NAME="lakehouse-s3-${ENVIRONMENT}"

echo "Deploying infrastructure for environment: ${ENVIRONMENT}"

aws cloudformation deploy \
  --template-file infrastructure/s3-buckets.yaml \
  --stack-name ${STACK_NAME} \
  --parameter-overrides Environment=${ENVIRONMENT} \
  --capabilities CAPABILITY_IAM \
  --region us-east-1

echo "Infrastructure deployment completed!"
