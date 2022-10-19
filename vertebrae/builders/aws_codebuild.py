from collections import namedtuple
from enum import Enum

import boto3
import time
from botocore.exceptions import ProfileNotFound

region = 'us-east-1'
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


class ArtifactType(str, Enum):
    LIBRARY = 'lib'
    BINARY = 'exe'


class AwsCodeBuild:
    def __init__(self, s3):
        # self.log = log
        # logging.getLogger('awscodebuild').setLevel(logging.INFO)
        self.client = None
        self.s3 = s3

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
        self.client = session.client(service_name='codebuild', region_name=region)

    @classmethod
    def __generate_buildspec(cls, dcf_name: str, target: str, out_file: str, artifact_type: ArtifactType):
        ext = dcf_name[dcf_name.rindex('.')+1:]
        build_command = build_commands[ext][artifact_type.value].format(dcf_name, target, out_file)
        build_spec = build_spec_template.format(build_command, out_file)
        return build_spec

    GetProjectRequest = namedtuple('GetProject', 'project_name source_s3 destination_s3 serviceRole environment')

    def get_project(self, req: GetProjectRequest) -> str:
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
            print(f'create_project:\n{res}\n')
            return res['project']['name']
        except self.client.exceptions.ResourceAlreadyExistsException as e:
            return req.project_name

    StartBuildRequest = namedtuple('StartBuildRequest', 'project_name target source_dir_s3 source_file_s3 artifact_filename_s3 artifact_type')

    def start_build(self, req: StartBuildRequest):
        res = self.client.start_build(
            projectName=req.project_name,
            buildspecOverride=self.__generate_buildspec(req.source_file_s3, req.target, req.artifact_filename_s3, req.artifact_type),
            sourceLocationOverride=req.source_dir_s3,
        )
        print(f'start_build:\n{res}\n')
        return res['build']['id']

    def wait_for_builds(self, build_ids: [str], sleep_between_get: int):
        print(f'waiting for: {build_ids}')
        res = self.client.batch_get_builds(ids=build_ids)
        print(f'batch_get_builds:\n{res}\n')
        builds_to_watch = [build for build in res['builds'] if build['id'] in build_ids]
        build_done = [build for build in builds_to_watch if build['buildStatus'] != 'IN_PROGRESS']
        if len(build_done) == len(build_ids):
            return len([build for build in build_done if build['buildStatus'] == 'SUCCEEDED']) == len(build_ids)
        time.sleep(sleep_between_get)
        return self.wait_for_builds([build['id'] for build in builds_to_watch if build['buildStatus'] == 'IN_PROGRESS'],
                                    sleep_between_get)


def get_s3():
    def load_profile(profile='default'):
        try:
            boto3.session.Session(profile_name=profile)
            boto3.setup_default_session(profile_name=profile)
            return profile
        except ProfileNotFound:
            return None

    session = boto3.session.Session(profile_name=load_profile())
    return session.client(service_name='s3', region_name=region)


def start_build(cb: AwsCodeBuild, s3, account_id: str, project_name: str, bucket: str, dcf_name: str, target: str, artifact_type: ArtifactType):
    ext = '.so' if artifact_type == ArtifactType.BINARY else '.exe'
    dcf_dir = dcf_name.replace(".", "_")
    out_file = dcf_name.replace('.c', f'-{target[:target.index("-")]}{ext}')

    resp = s3.delete_object(Bucket=bucket, Key=f'{account_id}/dst/{out_file}')
    print(f'delete_object:\n{resp}\n')

    return cb.start_build(
        AwsCodeBuild.StartBuildRequest(
            project_name=project_name,
            target=target,
            source_dir_s3=f'{bucket}/{account_id}/src/{dcf_dir}/',
            source_file_s3=dcf_name,
            artifact_filename_s3=out_file,
            artifact_type=artifact_type
        )
    )


if __name__ == '__main__':
    s3 = get_s3()
    cb = AwsCodeBuild(s3)
    cb.connect()

    bucket = 'prelude-account-local'
    role_to_use = 'arn:aws:iam::231489180083:role/AwsCodeBuildAdminRole'

    account_id = 'foo'
    dcf = '324829a8-9ba9-4559-9b97-4e4cc0cc3bf4_linux.c'
    target = 'x86_64-linux-gnu'

    project_name = cb.get_project(AwsCodeBuild.GetProjectRequest(
        project_name=account_id,
        source_s3=f'{bucket}/{account_id}/src/',
        destination_s3=f'{bucket}/{account_id}/dst/',
        serviceRole=role_to_use,
        environment=dict(
            # type='LINUX_CONTAINER',
            type='ARM_CONTAINER',
            image='231489180083.dkr.ecr.us-west-1.amazonaws.com/compile:latest',
            computeType='BUILD_GENERAL1_SMALL',
            imagePullCredentialsType='SERVICE_ROLE'
        ))
    )
    build_ids = [start_build(cb, s3, account_id, project_name, bucket, dcf, target, ArtifactType.LIBRARY),
                 start_build(cb, s3, account_id, project_name, bucket, dcf, target, ArtifactType.BINARY)]
    is_build_success = cb.wait_for_builds(build_ids, 5)
    print(f'is success: {is_build_success}')
