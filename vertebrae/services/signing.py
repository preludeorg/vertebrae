import hashlib
import hmac
from typing import Optional

from vertebrae.service import Service


class SigningService(Service):

    def __init__(self, name):
        super().__init__(name)

    @staticmethod
    def __compute_digest(data: str, entropy: Optional[str]) -> str:
        h = hashlib.sha3_512()
        h.update(data)
        if entropy:
            h.update(entropy)
        return h.hexdigest()

    async def sign(self, data: str, entropy: Optional[str]) -> str:
        return self.__compute_digest(data, entropy)

    async def verify(self, expected_signature: str, data: str, entropy: Optional[str]) -> bool:
        return hmac.compare_digest(self.__compute_digest(data, entropy), expected_signature)
