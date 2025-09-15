#!/usr/bin/env python3
import os
import aws_cdk as cdk
from data_pipeline.data_pipeline_stack import DataPipelineStack

app = cdk.App()

# Define the stack with appropriate naming
DataPipelineStack(
    app,
    "DataPipelineStack",
    # Uncomment and modify as needed
    # env=cdk.Environment(account=os.getenv('CDK_DEFAULT_ACCOUNT'), region=os.getenv('CDK_DEFAULT_REGION')),
)

app.synth()
