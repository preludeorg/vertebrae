import os
import asyncio
import concurrent.futures
import logging

from typing import Optional

import boto3
import botocore
from botocore.exceptions import ProfileNotFound, BotoCoreError

from vertebrae.config import Config


class S3:

    def __init__(self, log):
        self.log = log
        logging.getLogger('s3transfer').setLevel(logging.INFO)
        self.client = None

    async def connect(self):
        """ Establish a connection to AWS """
        aws = Config.find('aws')
        if aws:
            def load_profile(profile='default'):
                try:
                    boto3.session.Session(profile_name=profile)
                    boto3.setup_default_session(profile_name=profile)
                    return profile
                except ProfileNotFound:
                    return None

            session = boto3.session.Session(profile_name=load_profile())
            self.client = session.client(service_name='s3', region_name=Config.find('aws')['region'])

    async def read(self, filename: str) -> str:
        """ Read file from S3 """
        bucket, key = filename.split('/', 1)
        try:
            body = self.client.get_object(Bucket=bucket, Key=key)
            return body['Body'].read()
        except self.client.exceptions.NoSuchKey:
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
            obj_list = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix).get('Contents')
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
