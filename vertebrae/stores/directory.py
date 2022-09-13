import aiofiles

from vertebrae.config import Config


class Directory:

    def __init__(self):
        self.name = None

    async def connect(self) -> None:
        self.name = Config.find('directory')

    async def read(self, filename: str):
        async with aiofiles.open(f'{self.name}/{filename}', mode='r') as f:
            return await f.read()

    async def write(self, filename: str, contents: str):
        async with aiofiles.open(f'{self.name}/{filename}', mode='w') as outfile:
            await outfile.write(contents)
