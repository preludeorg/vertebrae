from aiobotocore.session import AioSession

from vertebrae.config import Config


class AWS:

    @classmethod
    def client(cls, service: str):
        if aws := Config.find('aws'):
            session = AioSession(profile=aws.get('profile'))
            return session.create_client(service_name=service, region_name=Config.find('aws')['region'])
