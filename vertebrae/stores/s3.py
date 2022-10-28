import asyncio
import concurrent.futures
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

    async def connect(self):
        """ Establish a connection to AWS """
        self.client = AWS.client('s3')

    async def exists(self, bucket: str, object: str):
        """ Check if a file exists """
        try:
            self.client.head_object(Bucket=bucket, Key=object)
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
            body = self.client.get_object(Bucket=bucket, Key=key)
            return body['Body'].read()
        except self.client.exceptions.NoSuchKey:
            self.log.error(f'Missing {key}')
        except botocore.exceptions.ClientError:
            self.log.error(f'Missing {key}')

    def download_file(self, filename: str, dst: str):
        bucket, key = filename.split('/', 1)
        try:
            self.client.download_file(bucket, key, dst)
        except self.client.exceptions.NoSuchKey:
            self.log.error(f'Missing {key}')

    def upload_file(self, src: str, filename: str):
        bucket, key = filename.split('/', 1)
        try:
            self.client.upload_file(src, bucket, key)
        except FileNotFoundError:
            self.log.error(f'Missing {src}')

    async def write(self, filename: str, contents: str) -> None:
        """ Write file to S3 """
        bucket, key = filename.split('/', 1)
        self.client.put_object(Body=contents, Bucket=bucket, Key=key)

    async def delete(self, filename: str) -> None:
        """ Delete file from S3 """
        bucket, key = filename.split('/', 1)
        self.client.delete_object(Bucket=bucket, Key=key)

    async def walk(self, bucket: str, prefix: str) -> [str]:
        """ Get all files of S3 bucket """
        try:
            obj_list = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix).get('Contents', [])
            return [f['Key'] for f in obj_list]
        except botocore.exceptions.ConnectionClosedError:
            self.log.error('Failed connection to AWS S3')

    async def read_all(self, bucket: str, prefix: str) -> [str]:
        """ Read all contents of S3 bucket """
        def _retrieve(k):
            try:
                cfg = self.client.get_object(Bucket=bucket, Key=k)
                return k, cfg['Body'].read()
            except Exception:
                return None

        my_files = dict()
        try:
            files = await self.walk(bucket=bucket, prefix=prefix)
            if not files:
                return my_files

            tasks = []
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
            for f in files:
                tasks.append(asyncio.get_event_loop().run_in_executor(executor, _retrieve, f))
            completed, pending = await asyncio.wait(tasks)
            for task in completed:
                key, contents = task.result()
                if contents:
                    my_files[os.path.basename(key)] = contents
            return my_files
        except botocore.exceptions.ConnectionClosedError:
            self.log.error('Failed connection to AWS S3')

    def redirect_url(self, bucket: str, object_name: str, expires_in=60) -> Optional[str]:
        """ Generate a time-bound redirect URL to a specific file in a bucket """
        try:
            return self.client.generate_presigned_url(ClientMethod='get_object',
                                                      Params=dict(Bucket=bucket, Key=object_name),
                                                      ExpiresIn=expires_in,
                                                      HttpMethod='GET')
        except BotoCoreError:
            raise FileNotFoundError('Cannot find file. Make sure your requested version is correct.')
