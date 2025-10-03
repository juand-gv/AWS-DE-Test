from aws_cdk import (
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_glue as glue,
    aws_athena as athena,
    aws_lakeformation as lf,
    Duration,
    RemovalPolicy,
    Stack,
    CfnOutput,
    Tags,
)
from constructs import Construct
from pathlib import Path
import os

HERE = Path(__file__).resolve().parent

class DataPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        Tags.of(self).add("project", "aws-de-test")
        Tags.of(self).add("owner", "morbid@example.com")

        # -------------------------
        # Buckets
        # -------------------------
        data_bucket = s3.Bucket(self, "DataBucket",
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN,
            encryption=s3.BucketEncryption.S3_MANAGED
        )

        results_bucket = s3.Bucket(self, "AthenaResultsBucket",
            removal_policy=RemovalPolicy.RETAIN,
            encryption=s3.BucketEncryption.S3_MANAGED
        )

        # -------------------------
        # Lambda execution role
        # -------------------------
        lambda_role = iam.Role(self, "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
        )
        lambda_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
        )

        # allow lambda to read/write objects in the data bucket
        data_bucket.grant_read_write(lambda_role)

        # -------------------------
        # Lambda Layer (conditional)
        # -------------------------
        # Path pointing to lambda/extractor/.build_layer (expected tree: .build_layer/python/...)
        lambda_layer_path = (HERE.parent.parent / "lambda" / "extractor" / ".build_layer").resolve()
        lambda_asset_path = (HERE.parent.parent / "lambda" / "extractor").resolve()

        layer = None
        if lambda_layer_path.exists() and any(lambda_layer_path.iterdir()):
            # only create layer if folder exists and is not empty
            layer = _lambda.LayerVersion(self, "ExtractorDependenciesLayer",
                # code=_lambda.Code.from_asset(str(lambda_layer_path)),
                code=_lambda.Code.from_asset(str(lambda_asset_path), exclude=[".build_layer", "tests", "*.md", "requirements.txt"]),
                compatible_runtimes=[_lambda.Runtime.PYTHON_3_10],
                description="Dependencies for extractor Lambda (packaged as layer)"
            )
        else:
            # If the layer folder is not available at synth time, skip creating the layer.
            # In CI pipelines you should ensure the build step populates .build_layer.
            self.node.add_warning(f"Lambda layer path {lambda_layer_path} not present or empty. Skipping LayerVersion creation.")

        # -------------------------
        # Lambda function with layer attached
        # -------------------------
        

        layers_list = [layer] if layer is not None else []

        extractor_fn = _lambda.Function(self, "ExtractorLambda",
            runtime=_lambda.Runtime.PYTHON_3_10,
            handler="lambda_function.handler",
            code=_lambda.Code.from_asset(str(lambda_asset_path)),
            timeout=Duration.minutes(2),
            memory_size=512,
            role=lambda_role,
            environment={
                # indicate bucket and prefix
                "BUCKET": data_bucket.bucket_name,
                "PREFIX": "raw/",
                # public API (allowed by test spec)
                "API_URL": "https://randomuser.me/api/?results=100",
                # explicitly declare desired file format (implement in lambda code)
                "FILE_FORMAT": "parquet"
            },
            layers=layers_list
        )

        # -------------------------
        # Glue DB
        # -------------------------
        glue_db = glue.CfnDatabase(self, "GlueDatabase",
            catalog_id=self.account,
            database_input={"name": "users_db"}
        )

        # -------------------------
        # Glue Crawler role and crawler
        # -------------------------
        crawler_role = iam.Role(self, "GlueCrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ]
        )
        # Crawler needs read access to the bucket
        data_bucket.grant_read(crawler_role)

        # Use s3://bucket-name/raw/ as crawler target (Glue expects s3:// URIs)
        glue_crawler = glue.CfnCrawler(self, "UsersCrawler",
            role=crawler_role.role_arn,
            database_name=glue_db.ref,
            targets={"s3Targets": [{"path": f"s3://{data_bucket.bucket_name}/raw/"}]},
            # optionally you can add configuration or schedule here
        )

        # -------------------------
        # Athena Workgroup
        # -------------------------
        athena_workgroup = athena.CfnWorkGroup(self, "AthenaWorkgroup",
            name="users_workgroup",
            work_group_configuration={
                "resultConfiguration": {
                    "outputLocation": f"s3://{results_bucket.bucket_name}/athena-results/"
                }
            }
        )

        # -------------------------
        # Analytics / Athena role (principal) for LF permissions
        # -------------------------
        # Create a role that represents analysts or Athena queries that will read the catalog.
        # We allow athena.amazonaws.com as principal here; you could also create an IAM role assumed by users.
        analytics_role = iam.Role(self, "AnalyticsRole",
            assumed_by=iam.ServicePrincipal("athena.amazonaws.com"),
            description="Role representing analytics users / Athena queries for Lake Formation permissions"
        )

        # -------------------------
        # Lake Formation permissions
        # -------------------------
        # Grant LF permissions to the Glue crawler principal (so it can CREATE_TABLE, ALTER, DESCRIBE)
        lf.CfnPermissions(self, "GlueCrawlerLFPermissions",
            data_lake_principal={"dataLakePrincipalIdentifier": crawler_role.role_arn},
            resource={"databaseResource": {"name": glue_db.ref}},
            permissions=["CREATE_TABLE", "ALTER", "DESCRIBE"]
        )

        # Grant LF permissions to the Lambda principal if you plan to write directly and manage tables
        # (common pattern: give lambda write access to underlying S3 and, if needed, LF permissions to the table)
        lf.CfnPermissions(self, "LambdaLFPermissions",
            data_lake_principal={"dataLakePrincipalIdentifier": lambda_role.role_arn},
            resource={"databaseResource": {"name": glue_db.ref}},
            permissions=["DESCRIBE"]  # limit to what lambda needs; expand if required
        )

        # Grant LF SELECT/DESCRIBE to analytics role so Athena or analysts can query the tables
        lf.CfnPermissions(self, "AnalyticsLFPermissions",
            data_lake_principal={"dataLakePrincipalIdentifier": analytics_role.role_arn},
            resource={"databaseResource": {"name": glue_db.ref}},
            permissions=["SELECT", "DESCRIBE"]
        )

        # -------------------------
        # Outputs
        # -------------------------
        CfnOutput(self, "DataBucketName", value=data_bucket.bucket_name)
        CfnOutput(self, "GlueDatabaseName", value=glue_db.ref)
        CfnOutput(self, "AthenaResultsBucketName", value=results_bucket.bucket_name)
        CfnOutput(self, "ExtractorLambdaName", value=extractor_fn.function_name)
        if layer is not None:
            CfnOutput(self, "ExtractorLayerArn", value=layer.layer_version_arn)
        CfnOutput(self, "GlueCrawlerName", value=glue_crawler.ref)
        CfnOutput(self, "AnalyticsRoleArn", value=analytics_role.role_arn)
