#!/bin/bash

# Configuration
AWS_REGION="ap-northeast-1"
AWS_ACCOUNT_ID="339712857994"
ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

# Authenticate with ECR
aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin ${ECR_REGISTRY}

# Function to build and push image
build_and_push() {
    local DOCKERFILE=$1
    local IMAGE_NAME=$2
    
    echo "Building ${IMAGE_NAME}..."
    docker build -f ${DOCKERFILE} --platform linux/amd64 -t ${IMAGE_NAME} .
    
    echo "Tagging ${IMAGE_NAME}..."
    docker tag ${IMAGE_NAME}:latest ${ECR_REGISTRY}/${IMAGE_NAME}:latest
    
    echo "Pushing ${IMAGE_NAME}..."
    docker push ${ECR_REGISTRY}/${IMAGE_NAME}:latest
}

# Create repositories if they don't exist
for repo in lambda-binance-api-getter lambda-binance-ticker-generator lambda-binance-db-updater lambda-binance-coint-analyzer; do
    aws ecr describe-repositories --repository-names ${repo} 2>/dev/null || \
    aws ecr create-repository --repository-name ${repo} --region ${AWS_REGION}
done

# Build and push images
# build_and_push Dockerfile.binance_api_getter lambda-binance-api-getter
# build_and_push Dockerfile.binance_ticker_generator lambda-binance-ticker-generator
# build_and_push Dockerfile.binance_db_updater lambda-binance-db-updater
build_and_push Dockerfile.binance_coint_analyzer lambda-binance-coint-analyzer
echo "Done!"