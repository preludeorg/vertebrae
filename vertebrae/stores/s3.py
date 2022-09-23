import asyncio
import concurrent.futures
import logging
from pathlib import Path
from typing import Optional

import boto3
import botocore
from botocore.exceptions import ProfileNotFound, BotoCoreError

from vertebrae.config import Config


class S3:

    def __init__(self, log):
        self.log = log
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

    async def read(self, bucket: str, key: str) -> str:
        """ Read file from S3 """
        try:
            body = self.client.get_object(Bucket=bucket, Key=key)
            return body['Body'].read()
        except self.client.exceptions.NoSuchKey:
            self.log.error(f'Missing {key}')

    async def write(self, bucket: str, key: str, contents: str) -> None:
        """ Write file to S3 """
        self.client.put_object(Body=contents, Bucket=bucket, Key=key)

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
            tasks = []
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
            for key in self.client.list_objects_v2(Bucket=bucket, Prefix=prefix).get('Contents'):
                tasks.append(asyncio.get_event_loop().run_in_executor(executor, _retrieve, key['Key']))
            completed, pending = await asyncio.wait(tasks)
            for task in completed:
                key, contents = task.result()
                if contents:
                    my_files[Path(key).stem] = contents
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
