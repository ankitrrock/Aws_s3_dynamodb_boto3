import boto3
import json
import os
import time
from botocore.config import Config
from decouple import config
from django.conf import settings


class AwsBucketService:
    def __init__(self, bucket_name) -> None:
        self._bucket_name = bucket_name

    def get_bucket_resource(self):
        s3 = boto3.resource(
            "s3",
            aws_access_key_id=config("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=config("AWS_SECRET_ACCESS_KEY"),
            region_name=config("AWS_REGION"),
            config=Config(signature_version="s3v4"),
        )
        return s3

    def get_bucket_object(self, filelocation):
        s3 = self.get_bucket_resource()
        obj = s3.Object(self._bucket_name, filelocation)
        return obj

    def save_file_to_s3_bucket(self, data, filelocation):

        if isinstance(data, list):
            bucket_put_object = self.get_bucket_object(filelocation=filelocation)
            file_put_response = bucket_put_object.put(
                Body=bytes(json.dumps(data).encode("UTF-8"))
            )
        if isinstance(data, dict):
            bucket_put_object = self.get_bucket_object(filelocation=filelocation)
            file_put_response = bucket_put_object.put(
                Body=bytes(json.dumps(data).encode("UTF-8"))
            )
        return file_put_response

    # def download_s3_file(self, key, filepath=None):
    #     s3 = self.get_bucket_resource()
    #     bucket = s3.Bucket(self._bucket_name)
    #     if filepath is None:
    #         filepath = key.split("/")[-1]
    #         filepath = os.path.join(settings.JSON_FILE_STORAGE_FOR_S3_BUCKET, filepath)
    #     bucket.download_file(key, filepath)
    #     return filepath

    def save_csv_file_to_s3_bucket(self, s3_file_location, local_csv_file_location):
        bucket_put_object = self.get_bucket_object(filelocation=s3_file_location)
        with open(file=local_csv_file_location, mode="rb") as csv_file:

            file_put_response = bucket_put_object.put(Body=csv_file)
        return file_put_response

def s3_bucket_file_location(account_id: str, type_: str, screen_name: str, user_id):
    """
    returns file location for storing file in s3 bucket
    """
    time_stamp = time.time()
    if type_ == "tiktok_post":
        path = f"tiktok/{user_id}/{screen_name}/user/post/pr_{time_stamp}.json"
        return path


