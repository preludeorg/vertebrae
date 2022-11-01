![Vertebrae](https://user-images.githubusercontent.com/49954156/198859134-4f6d1c8f-a874-437f-bb75-2aa16e59f1f3.svg)

# Vertebrae

An application framework for building async Python micro services. 

Vertebrae is a security-focused, all purpose backbone for any Python API. It focuses on one core principle: reducing the lines of code in your application will enable you to write more consistent, more secure code.

> The rule of thumb is that every 1,000 lines of code has 15+ unknown bugs inside. The less code you have, the more stable and secure you can make it. 

Vertebrae helps reduce clutter by supplying three core components:

1. Routes: API handlers that supply stubs for accepting input via REST.
2. Databases: Async connection pooling to popular databases of each primary type: relational, file and cache.
3. Services: Connective tissue that creates an internal mesh network for your code.

## Install

```bash
pip install vertebrae
```

Read our [Vertebrae Framework introduction](https://feed.prelude.org/p/vertebrae) for a walk through.

## Quick start

Copy the [sample](sample) application and use it as a template for your own projects.

### How it works

Create a Vertebrae Server and attach Applications (APIs) to it with defined Vertebrae Routes:

```python
from app.routes.core_routes import CoreRoutes
from app.services.chat import ChatService
from vertebrae.config import Config
from vertebrae.core import Server, Application


if __name__ == '__main__':
    Config.load(Config.strip(env='conf/env.yml'))
    server = Server(
        applications=[
            Application(port=8079, routes=[CoreRoutes()]),
        ],
        services=[
            ChatService(name='chat')
        ])
    server.run()
```

Next, add route classes to accept API requests:

> Vertebrae supplies a /ping route for every Application by default, which is helpful if you have a load-balancer doing health checks.

```python
from aiohttp import web

from app.authentication import allowed
from vertebrae.core import Route
from vertebrae.service import Service


class CoreRoutes:

    def routes(self) -> [Route]:
        return [
            Route(method='GET', route='/chat', handle=self._get_messages),
            Route(method='POST', route='/chat', handle=self._save_message),
        ]

    @allowed
    async def _get_messages(self, data: dict) -> web.json_response:
        messages = await Service.find('chat').get_msgs()
        return web.json_response(messages)

    @allowed
    async def _save_message(self, data: dict) -> web.Response:
        await Service.find('chat').save_msg(msg=data.get('text'))
        return web.Response(status=200, text='Message saved!')
```

Finally, add service classes that contain your business logic:

```python
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

```

Provide authentication to your API routes through decorators. Pay attention to the special ```strip_request``` function, which uniformly pulls data from API requests whether they are in query parameters, POST properties or other, and sends them in as a ```data``` parameter into all route handlers:

```python
from functools import wraps
from aiohttp import web

from vertebrae.config import Config
from vertebrae.core import strip_request, create_log

log = create_log('api')


def allowed(func):
    @wraps(func)
    async def helper(*args, **params):
        if args[1].headers.get('token') != Config.find('token'):
            log.error('[API] Unauthorized request')
            return web.Response(status=403)

        params['data'] = await strip_request(request=args[1])
        return await func(args[0], **params)
    return helper
```

## Advanced

### Config

Vertebrae supplies a Config object to hold any application secrets or other key/value pairs used in your code. 

Store these values in a YML file and load it inside your Vertebrae Server, such as:

```python
Config.load(Config.strip(env='conf/env.yml'))
```

You can then look up Config values from anywhere inside your application through ```Config.find("my_val")```.

If you prefer environment variables, note that Vertebrae will look up all key names in your YML file and use the env var first, if found.

### Databases

Vertebrae supports the following databases, accessible from any service class:

- relational (Postgres)
- cache (Redis)
- directory (local file system)
- s3 (AWS S3 service)

To enable any, ensure your Config module contains the appropriate connection details. 

> Note that S3 requires a standard ~/.aws/credentials file to be accessible as well. 

Here is a complete listing:

```yaml
postgres:
  database: frequency
  user: postgres
  password: ~
  host: localhost
  port: 5432
redis:
  host: localhost
directory: /home/ubuntu
aws:
  region: us-west-1
```

### Cloud

Vertebrae contains a cloud module that supplies clients to popular cloud providers. 

> Currently AWS is the only supported provider. 

To access any AWS service:

```python
async with AWS.client('s3') as client:
    await client.list_objects_v2(...)
```

### Detect 

Prelude is beta testing a new continuous security testing service called Detect. This service is currently configured
inside Vertebrae as an option - however it is not enabled (nor is it possible to enable at this time). A future version
of this framework will make Detect available.
