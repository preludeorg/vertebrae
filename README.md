# Vertebrae

An application framework for building async Python microservices. Use this to add consistency between your API code bases.
You get an API, a mesh network of services, and database connection pooling.

## Install
```bash
pip install vertebrae
```

## How it works

1. Create a Vertebrae Server and attach applications (APIs) to it with defined Vertebrae Routes.
2. (optionally) Supply connection details to Postgres and/or Redis. Vertebrae will create async connection pools for each.
3. Convert your classes to Vertebrae Services. This creates a mesh network between them. Each service gets a handler to your databases.

## Get started

Start by loading any application properties into the ```Config``` object.
Then, create a server object containing ```applications``` and ```services```.
An application is an API that serves a list of route classes at a specified port. A service is a standalone
Python class that contains business logic.

```python
from vertebrae.config import Config
from vertebrae.core import Server, Application

if __name__ == '__main__':
    Config.load(Config.strip(env='conf/env.yml'))
    server = Server(
        applications=[
            Application(port=4000, routes=[MyRoutes()])
        ],
        services=[
            BasicService(),
            NotifyService()
        ])
    server.run()
```

### Example: Config

When the app boots, the ```Config``` object is injected with an arbitrary YML file of your choosing. Any environment
variables with matching property names will overwrite the values in the file. After the injection, reference any property 
within your app via ```Config.find(property)```.

> Below are the required properties if you wish to add Postgres or Redis databases.

```yaml
postgres:
  database: frequency
  user: postgres
  password: ~
  host: localhost
  port: 5432
redis:
  host: localhost
```

### Example: Service

Each service must create a logger. Optionally, they can attach a handler to the database.

```python
from vertebrae.service import Service

class BasicService(Service):
    """ General functionality for this app """

    def __init__(self):
        self.log = self.logger('basic')
        self.database = self.db()
    
    async def start(self):
        self.log.debug('Service start functions auto run when the app boots')
    
    async def get_account(self, name):
        self.log.info(f'Creating new account: {name}')
```

### Example: Route

A route class must contain a ```routes``` function which returns a list of ```Route``` objects.
These represent your API handlers. Note how services can be engaged throughout your application.

```python
from aiohttp import web

from vertebrae.core import Route
from vertebrae.service import Service

class MyRoutes:

    def routes(self) -> [Route]:
        return [
            Route(method='GET', route='/account', handle=self._get_account)
        ]

    async def _get_account(self, request: web.Request) -> web.json_response:
        resp = await Service.find('basic').get_account(name=request.rel_url.query['name'])
        return web.json_response(resp)
```
