#!/usr/bin/env python3
import aws_cdk as cdk
from stacks.data_pipeline_stack import DataPipelineStack

app = cdk.App()
DataPipelineStack(app, "DataPipelineStack",
    env=cdk.Environment(account="339712743071", region="us-east-1")
)
app.synth()
