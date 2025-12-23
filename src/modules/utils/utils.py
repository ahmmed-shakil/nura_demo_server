import io
import os
from typing import Sequence, Any
import boto3
from botocore.exceptions import ClientError
import pandas as pd
from sqlalchemy import TextClause, Row
from src.modules import Database, logger
from src.modules.config import settings

s3 = boto3.client(
    's3',
    aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
    region_name=settings.REGION_NAME
    )


def query_data(query: TextClause, params: dict | None = None) -> Sequence[Row[Any]]:
    """Query data from DB (sync).
    Args:
        query: SQL query
        params: SQL query parameters
    Returns:
        data: query response data
    """

    try:
        engine = Database.get_engine()
        with engine.connect() as connection:
            result = connection.execute(query, params)
            data = result.fetchall()
            logger.debug("Fetched data: %s", data)
    except Exception as err:
        logger.error("DB connection error. Error: %s", err)
        raise err
    return data


async def query_data_async(query: TextClause, params: dict | None = None) -> Sequence[Row[Any]]:
    """Query data from DB (async).
    Args:
        query: SQL query
        params: SQL query parameters
    Returns:
        data: query response data
    """

    try:
        async_engine = Database.get_async_engine()
        async with async_engine.connect() as connection:
            result = await connection.execute(query, params)
            data = result.fetchall()
            logger.debug("Fetched data: %s", data)
    except Exception as err:
        logger.error("DB connection error. Error: %s", err)
        raise err
    return data


async def query_df_async(query: TextClause, params: dict | None = None) -> pd.DataFrame:
    """Query data from DB (async) as pandas dataframe.
    Args:
        query: SQL query
        params: SQL query parameters
    Returns:
        data: query response data
    """

    data = await query_data_async(query, params)
    data = pd.DataFrame(data)
    return data


def get_media_file(aws_file_name: str):

    logger.info("Getting media file from S3")
    file_name = f"{settings.TEMP_PATH}/{os.path.basename(aws_file_name)}"
    aws_object = f"{settings.MEDIA_FILE_PATH}/{aws_file_name}"
    try:
        # s3.download_file(settings.BUCKET_NAME, aws_object, file_name)
        # logger.info("Downloaded %s from S3 bucket %s", file_name, settings.BUCKET_NAME)
        # return file_name
        file_obj = s3.get_object(Bucket=settings.BUCKET_NAME, Key=aws_object)
        return io.BytesIO(file_obj['Body'].read())
    # except ClientError as e:
    #     if e.response["Error"]["Code"] == "404":
    #         logger.error("File %s not found in S3 bucket %s", aws_object, settings.BUCKET_NAME)
    #         return None
    #     logger.error("Unable to download %s from S3 bucket %s", aws_object, settings.BUCKET_NAME)
    #     raise e
    except s3.exceptions.NoSuchKey:
        logger.error("File %s not found in S3 bucket %s", aws_object, settings.BUCKET_NAME)
        return None

async def store_images(file):
    print(file.filename) 
    aws_object = settings.FILE_PATH
    try : 
        s3.upload_fileobj(
            file.file,
        settings.BUCKET_NAME,
            f"{aws_object}/{file.filename}"
        )
        return file.filename
    except Exception as e:
        return None 
  
def get_image_capture(file_name: str):
    aws_object = settings.FILE_PATH
    try:
        file_obj = s3.get_object(Bucket=settings.BUCKET_NAME, Key=f"{aws_object}/{file_name}")
        return io.BytesIO(file_obj['Body'].read())
    except s3.exceptions.NoSuchKey:
        logger.error("File %s not found in S3 bucket %s", aws_object, settings.BUCKET_NAME)
        return None
