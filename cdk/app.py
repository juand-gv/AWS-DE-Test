#!/usr/bin/env python3
import aws_cdk as cdk
from stacks.data_pipeline_stack import DataPipelineStack
import os

app = cdk.App()

# Si CDK_DEFAULT_ACCOUNT no está definido, env será None y CDK usará las credenciales configuradas.
env = None
if os.getenv("CDK_DEFAULT_ACCOUNT"):
    env = cdk.Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"),
        region=os.getenv("CDK_DEFAULT_REGION", "us-east-1")
    )

DataPipelineStack(app, "DataPipelineStack", env=env)

app.synth()
