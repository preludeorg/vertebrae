import asyncio
import logging
from collections import namedtuple
from enum import Enum

import boto3
from botocore.exceptions import ProfileNotFound

from vertebrae.config import Config

build_spec_template = '''
version: 0.2

phases: 
  build:
    commands:
      - {}
artifacts:
  files:
    - {}
'''

build_commands = dict(
    c=dict(
      lib='zig build-lib {} -fPIC -dynamic --library c -target {} -femit-bin={}',
      exe='zig cc {} -Oz -target {} -o {}'
    )
)

CreateProjectRequest = namedtuple('GetProject', 'project_name source_s3 destination_s3 serviceRole environment')
StartBuildRequest = namedtuple('StartBuildRequest',
                               'project_name target source_dir_s3 source_file_s3 artifact_filename_s3 artifact_type')


class ArtifactType(str, Enum):
    LIBRARY = 'lib'
    BINARY = 'exe'


class AwsCodeBuild:
    def __init__(self, log):
        self.log = log
        logging.getLogger('awscodebuild').setLevel(logging.INFO)
        self.client = None

    def connect(self):
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
            self.client = session.client(service_name='codebuild', region_name=Config.find('aws')['region'])

    @classmethod
    def __generate_buildspec(cls, dcf_name: str, target: str, out_file: str, artifact_type: ArtifactType):
        ext = dcf_name[dcf_name.rindex('.')+1:]
        build_command = build_commands[ext][artifact_type.value].format(dcf_name, target, out_file)
        build_spec = build_spec_template.format(build_command, out_file)
        return build_spec

    def create_project(self, req: CreateProjectRequest) -> str:
        artifact_path_arr = req.destination_s3.split('/', 1)
        try:
            res = self.client.create_project(
                name=req.project_name,
                source=dict(
                    type="S3",
                    location=req.source_s3,
                ),
                artifacts=dict(
                    type="S3",
                    location=artifact_path_arr[0],
                    path=artifact_path_arr[1],
                    name='/',
                    packaging='NONE',
                ),
                environment=req.environment,
                serviceRole=req.serviceRole,
                timeoutInMinutes=5,
                queuedTimeoutInMinutes=5,
            )
            return res['project']['name']
        except self.client.exceptions.ResourceAlreadyExistsException as e:
            return req.project_name

    def delete_project(self, project_name: str) -> None:
        try:
            self.client.delete_project(project_name)
        except self.client.exceptions.InvalidInputException as e:
            self.log.error(f'Project does not exist: {e}')

    def start_build(self, req: StartBuildRequest):
        res = self.client.start_build(
            projectName=req.project_name,
            buildspecOverride=self.__generate_buildspec(req.source_file_s3, req.target, req.artifact_filename_s3, req.artifact_type),
            sourceLocationOverride=req.source_dir_s3,
        )
        return res['build']['id']

    def delete_builds(self, build_ids: [str]) -> [str]:
        resp = self.client.batch_delete_builds(ids=build_ids)
        return resp['buildsDeleted']

    async def wait_for_builds(self, build_ids: [str], sleep_between_get: int):
        res = self.client.batch_get_builds(ids=build_ids)
        builds_to_watch = [build for build in res['builds'] if build['id'] in build_ids]
        build_done = [build for build in builds_to_watch if build['buildStatus'] != 'IN_PROGRESS']
        if len(build_done) == len(build_ids):
            return len([build for build in build_done if build['buildStatus'] == 'SUCCEEDED']) == len(build_ids)
        await asyncio.sleep(sleep_between_get)
        return await self.wait_for_builds([build['id'] for build in builds_to_watch if build['buildStatus'] == 'IN_PROGRESS'],
                                    sleep_between_get)