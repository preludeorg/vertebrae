import boto3
from botocore.exceptions import ProfileNotFound

from vertebrae.config import Config


class AWS:

    @classmethod
    def client(cls, service: str):
        aws = Config.find('aws')
        if aws:
            session = boto3.session.Session(profile_name=cls.load_profile(aws.get('profile')))
            return session.client(service_name=service, region_name=Config.find('aws')['region'])

    @classmethod
    def resource(cls, service: str):
        aws = Config.find('aws')
        if aws:
            session = boto3.session.Session(profile_name=cls.load_profile(aws.get('profile')))
            return session.resource(service_name=service, region_name=Config.find('aws')['region'])

    @staticmethod
    def load_profile(profile='default'):
        try:
            boto3.session.Session(profile_name=profile)
            boto3.setup_default_session(profile_name=profile)
            return profile
        except ProfileNotFound:
            return None
