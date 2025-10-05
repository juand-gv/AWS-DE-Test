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

# Ajuste: HERE apunta al archivo y luego usamos HERE.parent para la raíz del proyecto
HERE = Path(__file__).resolve()

class DataPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        Tags.of(self).add("project", "aws-de-test")
        Tags.of(self).add("owner", "morbid@example.com")

        # -------------------------
        # Buckets con nombre fijo
        # -------------------------
        data_bucket = s3.Bucket(self, "DataBucket",
            bucket_name="datapipelinestack-data-bucket",
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            encryption=s3.BucketEncryption.S3_MANAGED
        )

        results_bucket = s3.Bucket(self, "AthenaResultsBucket",
            bucket_name="datapipelinestack-athenaresults-bucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            encryption=s3.BucketEncryption.S3_MANAGED
        )

        # -------------------------
        # Lambda execution role con nombre fijo
        # -------------------------
        lambda_role = iam.Role(self, "LambdaExecutionRole",
            role_name="datapipelinestack-ExtractorLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
        )
        lambda_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
        )
        data_bucket.grant_read_write(lambda_role)

        # -------------------------
        # Lambda Layer detection
        # -------------------------
        lambda_layer_path = (HERE.parent / "lambda" / "extractor" / ".build_layer").resolve()
        lambda_asset_path = (HERE.parent / "lambda" / "extractor").resolve()

        layer = None
        if lambda_layer_path.exists() and any(lambda_layer_path.iterdir()):
            layer = _lambda.LayerVersion(self, "ExtractorDependenciesLayer",
                layer_version_name="datapipelinestack-ExtractorLayer",
                code=_lambda.Code.from_asset(str(lambda_asset_path), exclude=[".build_layer", "tests", "*.md", "requirements.txt"]),
                compatible_runtimes=[_lambda.Runtime.PYTHON_3_10],
                description="Dependencies for extractor Lambda (packaged as layer)"
            )
        else:
            # Cambio: print en lugar de self.node.add_warning (que causa AttributeError)
            print(f"Lambda layer path {lambda_layer_path} not present or empty. Skipping LayerVersion creation.")

        layers_list = [layer] if layer is not None else []

        extractor_fn = _lambda.Function(self, "ExtractorLambda",
            function_name="datapipelinestack-ExtractorLambda",
            runtime=_lambda.Runtime.PYTHON_3_10,
            handler="lambda_function.handler",
            code=_lambda.Code.from_asset(str(lambda_asset_path)) if lambda_asset_path.exists() else _lambda.Code.from_inline(
                "def handler(event, context):\n  return {'statusCode':200, 'body':'no code'}"
            ),
            timeout=Duration.minutes(2),
            memory_size=512,
            role=lambda_role,
            environment={
                "BUCKET": data_bucket.bucket_name,
                "PREFIX": "raw/",
                "API_URL": "https://randomuser.me/api/?results=100",
                "FILE_FORMAT": "parquet"
            },
            layers=layers_list
        )

        # -------------------------
        # Glue DB con nombre fijo
        # -------------------------
        glue_db = glue.CfnDatabase(self, "GlueDatabase",
            catalog_id=self.account,
            database_input={"name": "users_db"}
        )

        # -------------------------
        # Glue Crawler Role
        # -------------------------
        crawler_role = iam.Role(self, "GlueCrawlerRole",
            role_name="datapipelinestack-GlueCrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")]
        )
        # permisos s3 para el crawler
        data_bucket.grant_read(crawler_role)

        # -------------------------
        # Analytics role (ej. Athena)
        # -------------------------
        analytics_role = iam.Role(self, "AnalyticsRole",
            role_name="datapipelinestack-AnalyticsRole",
            assumed_by=iam.ServicePrincipal("athena.amazonaws.com"),
            description="Role representing analytics users / Athena queries for Lake Formation permissions"
        )

        # -------------------------
        # Registrar S3 location en Lake Formation (evitar service-linked role)
        # Hacemos que LF use el crawler_role creado arriba para el registro.
        # -------------------------
        lf_data_location = lf.CfnResource(self, "LakeFormationDataLocation",
            resource_arn=f"arn:aws:s3:::{data_bucket.bucket_name}",
            role_arn=crawler_role.role_arn,
            use_service_linked_role=False
        )
        # Aseguramos que la CFN cree el role primero y luego registre la ubicacion LF
        lf_data_location.node.add_dependency(crawler_role)
        lf_data_location.node.add_dependency(data_bucket)

        # -------------------------
        # Lake Formation Data Location Permissions
        # -------------------------
        crawler_perm = lf.CfnPermissions(self, "CrawlerDataLocationPermissions",
            data_lake_principal={"dataLakePrincipalIdentifier": crawler_role.role_arn},
            resource={
                "dataLocationResource": {
                    "catalogId": self.account,
                    "resourceArn": f"arn:aws:s3:::{data_bucket.bucket_name}"
                }
            },
            permissions=["DATA_LOCATION_ACCESS"]
        )
        crawler_perm.node.add_dependency(lf_data_location)
        crawler_perm.node.add_dependency(crawler_role)

        analytics_perm = lf.CfnPermissions(self, "AnalyticsDataLocationPermissions",
            data_lake_principal={"dataLakePrincipalIdentifier": analytics_role.role_arn},
            resource={
                "dataLocationResource": {
                    "catalogId": self.account,
                    "resourceArn": f"arn:aws:s3:::{data_bucket.bucket_name}"
                }
            },
            permissions=["DATA_LOCATION_ACCESS"]
        )
        analytics_perm.node.add_dependency(lf_data_location)
        analytics_perm.node.add_dependency(analytics_role)

        # -------------------------
        # Lake Formation Database Permissions
        # -------------------------
        db_perm_crawler = lf.CfnPermissions(self, "GlueCrawlerLFPermissions",
            data_lake_principal={"dataLakePrincipalIdentifier": crawler_role.role_arn},
            resource={
                "databaseResource": {
                    "catalogId": self.account,
                    "name": glue_db.ref
                }
            },
            permissions=["CREATE_TABLE", "ALTER", "DESCRIBE"]
        )
        db_perm_crawler.node.add_dependency(lf_data_location)
        db_perm_crawler.node.add_dependency(crawler_role)
        db_perm_crawler.node.add_dependency(glue_db)

        db_perm_lambda = lf.CfnPermissions(self, "LambdaLFPermissions",
            data_lake_principal={"dataLakePrincipalIdentifier": lambda_role.role_arn},
            resource={
                "databaseResource": {
                    "catalogId": self.account,
                    "name": glue_db.ref
                }
            },
            permissions=["DESCRIBE"]
        )
        db_perm_lambda.node.add_dependency(lf_data_location)
        db_perm_lambda.node.add_dependency(lambda_role)
        db_perm_lambda.node.add_dependency(glue_db)

        db_perm_analytics = lf.CfnPermissions(self, "AnalyticsLFDatabasePermissions",
            data_lake_principal={"dataLakePrincipalIdentifier": analytics_role.role_arn},
            resource={
                "databaseResource": {
                    "catalogId": self.account,
                    "name": glue_db.ref
                }
            },
            permissions=["DESCRIBE"]
        )
        db_perm_analytics.node.add_dependency(lf_data_location)
        db_perm_analytics.node.add_dependency(analytics_role)
        db_perm_analytics.node.add_dependency(glue_db)

        # -------------------------
        # Table-level permissions (para todas las tablas en la DB)
        # -------------------------
        table_perm_analytics = lf.CfnPermissions(self, "AnalyticsLFTablePermissions",
            data_lake_principal={"dataLakePrincipalIdentifier": analytics_role.role_arn},
            resource={
                "tableResource": {
                    "catalogId": self.account,
                    "databaseName": glue_db.ref,
                    "tableWildcard": {}
                }
            },
            permissions=["SELECT", "DESCRIBE"]
        )
        table_perm_analytics.node.add_dependency(db_perm_analytics)
        table_perm_analytics.node.add_dependency(lf_data_location)

        # -------------------------
        # Glue Crawler (depende de que LF location y permisos existan)
        # -------------------------
        glue_crawler = glue.CfnCrawler(self, "UsersCrawler",
            name="datapipelinestack-UsersCrawler",
            role=crawler_role.role_arn,
            database_name=glue_db.ref,
            targets={"s3Targets": [{"path": f"s3://{data_bucket.bucket_name}/raw/"}]}
        )
        glue_crawler.add_dependency(glue_db)
        glue_crawler.node.add_dependency(crawler_perm)

        # -------------------------
        # Athena Workgroup
        # -------------------------
        athena_workgroup = athena.CfnWorkGroup(self, "AthenaWorkgroup",
            name="datapipelinestack-users-workgroup",
            work_group_configuration={
                "resultConfiguration": {
                    "outputLocation": f"s3://{results_bucket.bucket_name}/athena-results/"
                }
            }
        )
        athena_workgroup.node.add_dependency(results_bucket)

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
