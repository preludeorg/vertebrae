from aiobotocore.session import AioSession

from vertebrae.config import Config


class AWS:

    @classmethod
    def client(cls, service: str):
        session = AioSession(profile=Config.find('aws_profile'))
        return session.create_client(service_name=service, region_name=Config.find('aws_region'))
