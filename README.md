# Data Pipeline CDK (Python)

Repo template for the AWS CDK data pipeline technical test.

## What's included
- CDK app (Python) in `cdk/`
- Lambda extractor in `lambda/extractor/`
- GitHub Actions workflow in `.github/workflows/cdk-deploy.yml`
- Basic unit test in `tests/`

## Quickstart (local)
1. Fill AWS credentials locally or in GitHub Secrets (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_ACCOUNT_ID)
2. Bootstrap and deploy:
   ```bash
   cd cdk
   python -m pip install -r requirements.txt
   cdk bootstrap aws://$AWS_ACCOUNT_ID/$AWS_REGION
   cdk deploy --require-approval never
   ```
3. To run unit tests:
   ```bash
   python -m pip install -r lambda/extractor/requirements.txt
   pytest -q
   ```