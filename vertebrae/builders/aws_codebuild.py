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
      exe='zig cc {} -Oz, -target {} -o {}'
    )
)


class AwsCodeBuild:
    def __init__(self, s3):
        # self.log = log
        # logging.getLogger('awscodebuild').setLevel(logging.INFO)
        self.client = None
        self.s3 = s3
        self.bucket = 'prelude-account-local'
        self.role_to_use = 'arn:aws:iam::231489180083:role/AwsCodeBuildAdminRole'

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
    def __get_environment(cls):
        return dict(
            # type='LINUX_CONTAINER',
            type='ARM_CONTAINER',
            image='231489180083.dkr.ecr.us-west-1.amazonaws.com/compile:latest',
            computeType='BUILD_GENERAL1_SMALL',
            imagePullCredentialsType='SERVICE_ROLE'
        )

    @classmethod
    def __get_target(cls):
        target = 'x86_64-linux-gnu'
        return target

    @classmethod
    def get_file_extension(cls, dcf_name: str) -> str:
        return dcf_name[dcf_name.rindex('.')+1:]

    @classmethod
    def __get_out_file(cls, dcf_name: str, target: str):
        ext = cls.get_file_extension(dcf_name)
        out = dcf_name.replace(f'.{ext}', f'-{target[:target.index("-")]}.so')
        return out

    @classmethod
    def __get_buildspec(cls, dcf_name: str, target: str, out_file: str):
        ext = cls.get_file_extension(dcf_name)
        build_command = build_commands[ext]['lib'].format(dcf_name, target, out_file)
        build_spec = build_spec_template.format(build_command, out_file)
        return build_spec

    def get_project(self, account_id: str) -> str:
        project_name = account_id
        try:
            res = self.client.create_project(
                name=project_name,
                source=dict(
                    type="S3",
                    location=f'{self.bucket}/{account_id}/src/',
                ),
                artifacts=dict(
                    type="S3",
                    location=self.bucket,
                    path=f'{account_id}/dst/',
                    name='/',
                    packaging='NONE',
                ),
                environment=self.__get_environment(),
                serviceRole=self.role_to_use,
                timeoutInMinutes=5,
                queuedTimeoutInMinutes=5,
            )
            print(f'create_project:\n{res}\n')
            return res['project']['name']
        except self.client.exceptions.ResourceAlreadyExistsException as e:
            return project_name

    def start_build(self, account_id: str, project_name: str, dcf_name: str):
        target = self.__get_target()
        out_file = self.__get_out_file(dcf_name, target)
        resp = self.s3.delete_object(Bucket=self.bucket, Key=f'{account_id}/dst/{out_file}')
        print(f'delete_object:\n{resp}\n')

        aws_dcf_name = dcf_name.replace(".", "_")
        res = self.client.start_build(
            projectName=project_name,
            buildspecOverride=self.__get_buildspec(dcf_name, target, out_file),
            sourceLocationOverride=f'{self.bucket}/{account_id}/src/{aws_dcf_name}/',
        )
        print(f'start_build:\n{res}\n')
        return res['build']['id']

    def wait_for_build(self, build_id: str):
        builds = self.client.batch_get_builds(ids=[build_id])
        print(f'batch_get_builds:\n{builds}\n')
        current_build = next(filter(lambda build: build['id'] == build_id, builds['builds']))
        build_status = current_build['buildStatus']
        if build_status != 'IN_PROGRESS':
            return build_status == 'SUCCEEDED'
        time.sleep(5)
        return self.wait_for_build(build_id)


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


if __name__ == '__main__':
    cb = AwsCodeBuild(get_s3())
    cb.connect()

    account_id = 'foo'
    dcf = '324829a8-9ba9-4559-9b97-4e4cc0cc3bf4_linux.c'

    project_name = cb.get_project(account_id)
    build_id = cb.start_build(account_id, project_name, dcf)
    is_build_success = cb.wait_for_build(build_id)
    print(f'is success: {is_build_success}')
