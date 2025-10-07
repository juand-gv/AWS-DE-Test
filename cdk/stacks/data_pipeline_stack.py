from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_glue as glue,
    aws_athena as athena,
    aws_lakeformation as lf,
    CfnOutput
)
from constructs import Construct
import os

class DataPipelineStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # --- S3 ---
        data_bucket = s3.Bucket(self, "DataBucket",
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL
        )

        results_bucket = s3.Bucket(self, "AthenaResults",
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL
        )

        # --- Lambda ---
        lambda_role = iam.Role(self, "LambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
        )
        lambda_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
        )
        data_bucket.grant_read_write(lambda_role)

        extractor = _lambda.Function(self, "ExtractorFunction",
            runtime=_lambda.Runtime.PYTHON_3_10,
            handler="lambda_function.handler",
            code=_lambda.Code.from_asset("../lambda/extractor"),
            timeout=Duration.minutes(1),
            environment={
                "BUCKET": data_bucket.bucket_name,
                "API_URL": "https://randomuser.me/api/?results=100",
                "FILE_FORMAT": "parquet"
            },
            role=lambda_role
        )

        # --- Glue ---
        glue_db = glue.CfnDatabase(self, "GlueDB",
            catalog_id=self.account,
            database_input={"name": "users_db"}
        )

        crawler_role = iam.Role(self, "CrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AdministratorAccess")
            ]
        )


        lf.CfnDataLakeSettings(
            self, "DataPipelineDataLakeSettings",
            admins=[
                lf.CfnDataLakeSettings.DataLakePrincipalProperty(
                    data_lake_principal_identifier=crawler_role.role_arn
                )
            ]
        )

        lf.CfnPermissions(self, "GlueDBPerms",
            data_lake_principal={"dataLakePrincipalIdentifier": crawler_role.role_arn},
            resource={"databaseResource": {"name": "users_db"}},
            permissions=["CREATE_TABLE", "ALTER", "DESCRIBE"]
        )

        
        data_bucket.grant_read(crawler_role)

        glue.CfnCrawler(self, "UsersCrawler",
            role=crawler_role.role_arn,
            database_name=glue_db.ref,
            targets={"s3Targets": [{"path": f"s3://{data_bucket.bucket_name}/"}]},
            schema_change_policy={"deleteBehavior": "LOG", "updateBehavior": "UPDATE_IN_DATABASE"}
        )

        # --- Lake Formation ---
        lf.CfnResource(self, "LFDataLocation",
            resource_arn=data_bucket.bucket_arn,
            use_service_linked_role=False,
            role_arn=crawler_role.role_arn
        )

        lf.CfnPermissions(self, "CrawlerPerms",
            data_lake_principal={"dataLakePrincipalIdentifier": crawler_role.role_arn},
            resource={"databaseResource": {"name": "users_db"}},
            permissions=["ALL"]
        )

        lf.CfnPermissions(self, "LambdaPerms",
            data_lake_principal={"dataLakePrincipalIdentifier": crawler_role.role_arn},
            resource={"dataLocationResource": {"s3Resource": data_bucket.bucket_arn}},
            permissions=["DATA_LOCATION_ACCESS"]
        )

        # --- Athena ---
        athena.CfnWorkGroup(self, "AthenaWG",
            name="users_wg",
            work_group_configuration={
                "resultConfiguration": {
                    "outputLocation": f"s3://{results_bucket.bucket_name}/results/"
                }
            }
        )

        # --- Outputs ---
        CfnOutput(self, "DataBucketNameOutput", value=data_bucket.bucket_name)
        CfnOutput(self, "GlueDBNameOutput", value="users_db")
        CfnOutput(self, "LambdaNameOutput", value=extractor.function_name)
