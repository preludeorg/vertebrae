import os
import pathlib
from pathlib import Path

import aiofiles

from vertebrae.config import Config


class Directory:

    def __init__(self, log):
        self.log = log
        self.name = None

    async def connect(self) -> None:
        self.name = Config.find('directory', os.path.join(str(Path.home()), '.vertebrae'))
        Path(self.name).mkdir(parents=True, exist_ok=True)

    async def read(self, filename: str):
        filepath = Path(pathlib.PurePath(self.name, filename))
        async with aiofiles.open(filepath, mode='rb') as f:
            return await f.read()

    async def walk(self, bucket: str, prefix='*'):
        filepath = Path(pathlib.PurePath(self.name, bucket))
        for path in Path(filepath).glob(f'{prefix}*'):
            async with aiofiles.open(path, mode='rb') as _:
                yield os.path.basename(path)

    async def write(self, filename: str, contents: str):
        filepath = Path(pathlib.PurePath(self.name, filename))
        async with aiofiles.open(filepath, mode='wb') as outfile:
            await outfile.write(contents)

    async def delete(self, filename: str):
        filepath = Path(pathlib.PurePath(self.name, filename))
        os.remove(filepath)
