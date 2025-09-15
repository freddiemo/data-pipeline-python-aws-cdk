from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_glue as glue,
    aws_athena as athena,
    aws_lakeformation as lakeformation,
    aws_events as events,
    aws_events_targets as targets,
    Duration,
    RemovalPolicy,
    CfnOutput
)
from constructs import Construct


class DataPipelineStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create S3 bucket for data storage
        self.data_bucket = s3.Bucket(
            self, "DataPipelineDataBucket",
            bucket_name="data-pipeline-bucket-jsonplaceholder",
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="DeleteOldVersions",
                    noncurrent_version_expiration=Duration.days(30),
                    enabled=True
                )
            ]
        )

        # Create S3 bucket for Athena query results
        self.athena_results_bucket = s3.Bucket(
            self, "DataPipelineAthenaResultsBucket",
            bucket_name="data-pipeline-athena-results-jsonplaceholder",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="DeleteQueryResults",
                    expiration=Duration.days(7),
                    enabled=True
                )
            ]
        )

        # Create IAM role for Lambda function
        lambda_role = iam.Role(
            self, "DataPipelineLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ]
        )

        # Grant Lambda permissions to write to S3
        self.data_bucket.grant_write(lambda_role)

        # Create Lambda function for data extraction
        self.data_extractor_lambda = _lambda.Function(
            self, "DataPipelineDataExtractorLambda",
            function_name="data-pipeline-data-extractor",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="data_extractor.lambda_handler",
            code=_lambda.Code.from_asset("lambda_functions"),
            timeout=Duration.minutes(5),
            memory_size=256,
            role=lambda_role,
            environment={
                "BUCKET_NAME": self.data_bucket.bucket_name
            }
        )

        # Create Glue Database
        self.glue_database = glue.CfnDatabase(
            self, "DataPipelineGlueDatabase",
            catalog_id=self.account,
            database_input={
                "name": "data_pipeline_db",
                "description": "Database for data pipeline project"
            }
        )

        # Create IAM role for Glue Crawler
        glue_crawler_role = iam.Role(
            self, "DataPipelineGlueCrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ]
        )

        # Grant Glue Crawler permissions to read from S3
        self.data_bucket.grant_read(glue_crawler_role)

        # Create Glue Crawler
        self.glue_crawler = glue.CfnCrawler(
            self, "DataPipelineGlueCrawler",
            name="data-pipeline-crawler",
            role=glue_crawler_role.role_arn,
            database_name=self.glue_database.ref,
            targets={
                "s3Targets": [
                    {
                        "path": f"s3://{self.data_bucket.bucket_name}/raw-data/"
                    }
                ]
            },
            schedule={
                "scheduleExpression": "cron(0 2 * * ? *)"  # Run daily at 2 AM UTC
            },
            schema_change_policy={
                "updateBehavior": "UPDATE_IN_DATABASE",
                "deleteBehavior": "LOG"
            }
        )

        # Create Lake Formation settings
        lakeformation.CfnDataLakeSettings(
            self, "DataPipelineDataLakeSettings",
            admins=[
                lakeformation.CfnDataLakeSettings.DataLakePrincipalProperty(
                    data_lake_principal_identifier=lambda_role.role_arn
                ),
                lakeformation.CfnDataLakeSettings.DataLakePrincipalProperty(
                    data_lake_principal_identifier=glue_crawler_role.role_arn
                )
            ]
        )

        # Create IAM role for Athena users
        self.athena_role = iam.Role(
            self, "DataPipelineAthenaRole",
            assumed_by=iam.ServicePrincipal("athena.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonAthenaFullAccess")
            ]
        )

        # Grant Athena permissions to read from data bucket and write to results bucket
        self.data_bucket.grant_read(self.athena_role)
        self.athena_results_bucket.grant_read_write(self.athena_role)

        # Create Athena Workgroup
        self.athena_workgroup = athena.CfnWorkGroup(
            self, "DataPipelineAthenaWorkGroup",
            name="data-pipeline-workgroup",
            description="Workgroup for data pipeline queries",
            work_group_configuration={
                "resultConfiguration": {
                    "outputLocation": f"s3://{self.athena_results_bucket.bucket_name}/query-results/"
                },
                "enforceWorkGroupConfiguration": True,
                "publishCloudWatchMetrics": True
            }
        )

        # Create EventBridge rule to trigger Lambda daily
        lambda_schedule_rule = events.Rule(
            self, "DataPipelineLambdaScheduleRule",
            schedule=events.Schedule.cron(
                minute="0",
                hour="1",
                day="*",
                month="*",
                year="*"
            ),
            description="Trigger data extraction lambda daily at 1 AM UTC"
        )

        # Add Lambda as target to the EventBridge rule
        lambda_schedule_rule.add_target(
            targets.LambdaFunction(
                self.data_extractor_lambda,
                event=events.RuleTargetInput.from_object({
                    "bucket_name": self.data_bucket.bucket_name
                })
            )
        )

        # Grant EventBridge permission to invoke Lambda
        self.data_extractor_lambda.add_permission(
            "AllowEventBridge",
            principal=iam.ServicePrincipal("events.amazonaws.com"),
            source_arn=lambda_schedule_rule.rule_arn
        )

        # Stack Outputs
        CfnOutput(
            self, "DataBucketName",
            value=self.data_bucket.bucket_name,
            description="Name of the S3 bucket for data storage"
        )

        CfnOutput(
            self, "AthenaResultsBucketName",
            value=self.athena_results_bucket.bucket_name,
            description="Name of the S3 bucket for Athena query results"
        )

        CfnOutput(
            self, "GlueDatabaseName",
            value=self.glue_database.ref,
            description="Name of the Glue database"
        )

        CfnOutput(
            self, "GlueCrawlerName",
            value=self.glue_crawler.ref,
            description="Name of the Glue crawler"
        )

        CfnOutput(
            self, "AthenaWorkGroupName",
            value=self.athena_workgroup.ref,
            description="Name of the Athena workgroup"
        )

        CfnOutput(
            self, "LambdaFunctionName",
            value=self.data_extractor_lambda.function_name,
            description="Name of the data extractor Lambda function"
        )
