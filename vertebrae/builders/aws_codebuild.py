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
      - zig build-lib {} -fPIC -dynamic --library c -target {} -femit-bin={}
      # - echo Build started on `date`
  post_build:
    commands:
      - echo Entered the post_build phase...
      - echo Build completed on `date`
artifacts:
  files:
    - {}
'''


class AwsCodeBuild:
    def __init__(self):
        # self.log = log
        # logging.getLogger('awscodebuild').setLevel(logging.INFO)
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
        self.client = session.client(service_name='codebuild', region_name='us-east-1')

    def create_project(self, account_id: str, dcf_name: str):
        # TODO figure out environment.type
        ext = dcf_name[dcf_name.rindex('.')+1:]
        target = 'x86_64-linux-gnu'
        out = dcf_name.replace(f'.{ext}', f'-{target[:target.index("-")]}.so')
        build_spec = build_spec_c.format(dcf_name, target, out, out)

        aws_dcf_name = dcf_name.replace(".", "_")
        project_name = f'{account_id}_{aws_dcf_name}'
        try:
            res = self.client.create_project(
                name=project_name,
                source=dict(
                    type="S3",
                    location=f'prelude-account-local/{account_id}/src/{aws_dcf_name}/',
                    buildspec=build_spec,
                ),
                artifacts=dict(
                    type="S3",
                    location=f'prelude-account-local',
                    path=f'{account_id}/dst/',
                    name='/',
                    packaging='NONE',
                ),
                environment=dict(
                    # type='LINUX_CONTAINER',
                    type='ARM_CONTAINER',
                    image='231489180083.dkr.ecr.us-west-1.amazonaws.com/compile:latest',
                    computeType='BUILD_GENERAL1_SMALL',
                    imagePullCredentialsType='SERVICE_ROLE'
                ),
                serviceRole='arn:aws:iam::231489180083:role/AwsCodeBuildAdminRole',
                timeoutInMinutes=5,
                queuedTimeoutInMinutes=5,
            )
            print(f'create_project:\n{res}\n')
            return res['project']['name']
        except self.client.exceptions.ResourceAlreadyExistsException as e:
            return project_name

    def start_build(self, project_name: str):
        res = self.client.start_build(
            projectName=project_name
        )
        print(f'start_build:\n{res}\n')


if __name__ == '__main__':
    cb = AwsCodeBuild()
    cb.connect()
    project_name = cb.create_project('foo', '324829a8-9ba9-4559-9b97-4e4cc0cc3bf4_linux.c')
    cb.start_build(project_name)
