AWS_ACCESS_KEY_ID ?= $(shell aws configure get aws_access_key_id $(AWS_FLAG))
AWS_SECRET_ACCESS_KEY ?= $(shell aws configure get aws_secret_access_key $(AWS_FLAG))
AWS_REGION ?= $(shell aws configure get region)
AWS_DYNAMODB_TABLE ?= "dagit-dev"
AWS_DYNAMODB_KEY ?= "id"

BUILD_ENV ?= AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) AWS_REGION=$(AWS_REGION) AWS_DYNAMODB_TABLE=$(AWS_DYNAMODB_TABLE) AWS_DYNAMODB_KEY=$(AWS_DYNAMODB_KEY)

test:
	$(BUILD_ENV) cargo test -- --nocapture
