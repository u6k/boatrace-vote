import os
from logging import config, getLogger

import boto3

#
# ログ設定
#

config.fileConfig("./logging.conf")


def get_logger(task_name):
    return getLogger(f"boatrace_vote.{task_name}")


#
# S3ストレージ関係
#

class S3Storage:
    def __init__(self):
        self._s3_endpoint = os.environ["AWS_ENDPOINT_URL"]
        self._s3_access_key = os.environ["AWS_ACCESS_KEY_ID"]
        self._s3_secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
        self._s3_bucket = os.environ["AWS_S3_BUCKET"]

        # Setup s3 client
        self.s3_client = boto3.resource(
            "s3",
            endpoint_url=self._s3_endpoint,
            aws_access_key_id=self._s3_access_key,
            aws_secret_access_key=self._s3_secret_key
        )

        self.s3_bucket_obj = self.s3_client.Bucket(self._s3_bucket)
        if not self.s3_bucket_obj.creation_date:
            self.s3_bucket_obj.create()

    def get_object(self, key):
        s3_obj = self.s3_bucket_obj.Object(key)

        obj = s3_obj.get()["Body"].read()

        return obj

    def put_object(self, key, obj):
        self.s3_bucket_obj.Object(key).put(Body=obj)
