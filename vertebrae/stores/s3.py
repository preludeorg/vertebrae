import asyncio
import logging
import os
from typing import Optional

import botocore
from botocore.exceptions import ProfileNotFound, BotoCoreError

from vertebrae.cloud.aws import AWS


class S3:

    def __init__(self, log):
        self.log = log
        logging.getLogger('s3transfer').setLevel(logging.INFO)
        self.client = None

    async def exists(self, bucket: str, object: str):
        """ Check if a file exists """
        try:
            async with AWS.client('s3') as client:
                await client.head_object(Bucket=bucket, Key=object)
                return True
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                self.log.warning(f'Missing {object}')
            else:
                self.log.error(f'Error looking up {object}')

    async def read(self, filename: str) -> str:
        """ Read file from S3 """
        bucket, key = filename.split('/', 1)
        try:
            async with AWS.client('s3') as client:
                body = await client.get_object(Bucket=bucket, Key=key)
                return await body['Body'].read()
        except await self.client.exceptions.NoSuchKey:
            self.log.error(f'Missing {key}')
        except botocore.exceptions.ClientError:
            self.log.error(f'Missing {key}')

    async def download_file(self, filename: str, dst: str):
        bucket, key = filename.split('/', 1)
        try:
            async with AWS.client('s3') as client:
                await client.download_file(bucket, key, dst)
        except await self.client.exceptions.NoSuchKey:
            self.log.error(f'Missing {key}')

    async def upload_file(self, src: str, filename: str):
        bucket, key = filename.split('/', 1)
        try:
            async with AWS.client('s3') as client:
                await client.upload_file(src, bucket, key)
        except FileNotFoundError:
            self.log.error(f'Missing {src}')

    @staticmethod
    async def write(filename: str, contents: str) -> None:
        """ Write file to S3 """
        bucket, key = filename.split('/', 1)
        async with AWS.client('s3') as client:
            await client.put_object(Body=contents, Bucket=bucket, Key=key)

    @staticmethod
    async def delete(filename: str) -> None:
        """ Delete file from S3 """
        bucket, key = filename.split('/', 1)
        async with AWS.client('s3') as client:
            await client.delete_object(Bucket=bucket, Key=key)

    async def walk(self, bucket: str, prefix: str) -> [str]:
        """ Get all files of S3 bucket """
        try:
            async with AWS.client('s3') as client:
                obj_list = await client.list_objects_v2(Bucket=bucket, Prefix=prefix)
                return [f['Key'] for f in obj_list.get('Contents', [])]
        except botocore.exceptions.ConnectionClosedError:
            self.log.error('Failed connection to AWS S3')

    async def read_all(self, bucket: str, prefix: str) -> [str]:
        """ Read all contents of S3 bucket """
        async def _retrieve(k):
            try:
                async with AWS.client('s3') as client:
                    cfg = await client.get_object(Bucket=bucket, Key=k)
                    return k, await cfg['Body'].read()
            except Exception:
                return None

        my_files = dict()
        try:
            if files := await self.walk(bucket=bucket, prefix=prefix):
                completed, pending = await asyncio.wait([_retrieve(f) for f in files])
                for task in completed:
                    key, contents = task.result()
                    if contents:
                        my_files[os.path.basename(key)] = contents
            return my_files
        except botocore.exceptions.ConnectionClosedError:
            self.log.error('Failed connection to AWS S3')

    @staticmethod
    async def redirect_url(bucket: str, object_name: str, expires_in=60) -> Optional[str]:
        """ Generate a time-bound redirect URL to a specific file in a bucket """
        try:
            async with AWS.client('s3') as client:
                return await client.generate_presigned_url(ClientMethod='get_object',
                                                           Params=dict(
                                                               Bucket=bucket, Key=object_name),
                                                           ExpiresIn=expires_in,
                                                           HttpMethod='GET')
        except BotoCoreError:
            raise FileNotFoundError(
                'Cannot find file. Make sure your requested version is correct.')
