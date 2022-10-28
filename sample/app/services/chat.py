from vertebrae.service import Service


class ChatService(Service):
    """ Talk through a Redis queue """

    def __init__(self, name):
        super().__init__(name)
        self.cache = self.db('cache')

    async def save_msg(self, msg: str) -> None:
        """ Save a message to the Redis queue """
        self.log.debug('Saving new message')
        await self.cache.set('voicemail', msg)

    async def get_msgs(self) -> [str]:
        """ Retrieve a message file the Redis queue """
        self.log.debug(f'Retrieving messages')
        messages = await self.cache.get_del('voicemail')
        return messages
