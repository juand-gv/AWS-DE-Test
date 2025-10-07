# Data Pipeline CDK (Python)

Repo template for the AWS CDK data pipeline technical test.

## What's included
- CDK app (Python) in `cdk/`
- Lambda extractor in `lambda/extractor/`



## Personal useful commands


Lambda layer

```Bash
pip install --platform manylinux2014_aarch64 `
               --target=./python/lib/python3.12/site-packages `
               --implementation cp `
               --python-version 3.12 `
               --only-binary=:all: `
               --upgrade `
               -r requirements_layer.txt
```






