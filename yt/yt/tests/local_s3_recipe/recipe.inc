# Use these variables in your tests to access the S3.
DEFAULT(AWS_ENDPOINT_URL http://127.0.0.1:8080)
ENV(AWS_ENDPOINT_URL=$AWS_ENDPOINT_URL)

DEFAULT(AWS_REGION eu-north-1)
ENV(AWS_REGION=$AWS_REGION)

DEFAULT(AWS_ACCESS_KEY_ID S3_PROXY_IDENTITY)
ENV(AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID)

DEFAULT(AWS_SECRET_ACCESS_KEY S3_PROXY_CREDENTIAL)
ENV(AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY)

DEPENDS(
    yt/yt/tests/local_s3_recipe
)

DATA(
    arcadia/yt/yt/tests/local_s3_recipe/bin/s3proxy
)

USE_RECIPE(
    yt/yt/tests/local_s3_recipe/local_s3_recipe
)
