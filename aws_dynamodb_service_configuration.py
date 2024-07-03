import boto3
from decouple import config
from typing import List, Dict
from main.helpers.logger_info import info_logger, error_logger
from botocore.exceptions import ClientError
import uuid


class AwsDynamoDbService:
    def __init__(self, table_name) -> None:
        self._tabel_name = table_name

    def get_dynamodb_resource(self):
        dynamodb = boto3.resource(
            "dynamodb",
            aws_access_key_id=config("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=config("AWS_SECRET_ACCESS_KEY"),
            region_name=config("AWS_REGION"),
        )
        return dynamodb

    def get_table_object(self):
        dynamodb_resource = self.get_dynamodb_resource()
        table = dynamodb_resource.Table(self._tabel_name)
        return table

    def store_data(self, data):
        tabel = self.get_table_object()
        try:
            with tabel.batch_writer() as batch:
                for obj in data:
                    batch.put_item(Item=obj)
        except ClientError as dynamo_db_store_data_error:
            error_logger.error(dynamo_db_store_data_error)

    def store_tiktok_post(self, data: List, account_handle, account_id):
        tabel = self.get_table_object()
        try:
            with tabel.batch_writer(overwrite_by_pkeys=["tiktok_post_id", "tiktok_user_id"]) as batch:
                for obj in data:
                    for row in obj:
                        datacheck={
                            "tiktok_post_id":str(row['id']),
                            "tiktok_user_id":str(account_id)
                        }
                        datacheck.update(**row)
                        batch.put_item(Item=datacheck)
        except ClientError as dynamo_db_store_data_error:
            error_logger.error(dynamo_db_store_data_error)

