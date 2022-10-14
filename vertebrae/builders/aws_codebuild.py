import json
import logging

import boto3
from botocore.exceptions import ProfileNotFound

build_spec_c = '''
version: 0.2

phases:
  install:
    commands:
      - echo Entered the install phase... 
  pre_build:
    commands:
      - echo Entered the pre_build phase... 
  build:
    commands:
      - echo Entered the build phase...
      - echo Build started on `date`
  post_build:
    commands:
      - echo Entered the post_build phase...
      - echo Build completed on `date`
'''


class AwsCodeBuild:
    def __init__(self):
        # self.log = log
        logging.getLogger('awscodebuild').setLevel(logging.INFO)
        self.client = None

    def connect(self):
        """ Establish a connection to AWS """
        # aws = Config.find('aws')
        # if aws:
        def load_profile(profile='default'):
            try:
                boto3.session.Session(profile_name=profile)
                boto3.setup_default_session(profile_name=profile)
                return profile
            except ProfileNotFound:
                return None

        session = boto3.session.Session(profile_name=load_profile())
        self.client = session.client(service_name='codebuild', region_name='us-west-1')


    def create_project(self, account_id: str, dcf_name: str):
        # TODO figure out environment.type
        dcf_name = dcf_name.replace(".", "_")
        res = self.client.create_project(
            name=f'{account_id}_{dcf_name}',
            source=dict(
                type="S3",
                location=f'prelude-account-local/{account_id}/src/{dcf_name}/',
                buildspec=build_spec_c,
            ),
            artifacts=dict(
                type="S3",
                location=f'prelude-account-local',
                path=f'{account_id}/dst/{dcf_name}/',
                name='/',
                packaging='NONE',
            ),
            environment=dict(
                type='LINUX_GPU_CONTAINER',
                image='231489180083.dkr.ecr.us-west-1.amazonaws.com/compile@latest',
                computeType='BUILD_GENERAL1_SMALL',
            ),
            serviceRole='arn:aws:iam::231489180083:role/AWSCodeBuildAdminAccess',
            timeoutInMinutes=5,
            queuedTimeoutInMinutes=5,
        )
        print(f'create_project:\n{json.dumps(res)}\n')
        return res.project.name

    def start_build(self, project_name: str):
        res = self.client.start_build(
            projectName=project_name
        )
        print(f'start_build:\n{json.dumps(res)}\n')


if __name__ == '__main__':
    cb = AwsCodeBuild()
    cb.connect()
    project_name = cb.create_project('foo', '324829a8-9ba9-4559-9b97-4e4cc0cc3bf4_linux.c')
    cb.start_build(project_name)