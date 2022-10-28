# Vertebrae

An application framework for building async Python microservices. Use this to add consistency between your API code bases.
You get an API, a mesh network of services, and database connection pooling.

## Install

```bash
pip install vertebrae
```

Read our [Vertebrae Framework introduction](https://feed.prelude.org/p/vertebrae) for a comprehensive walk through.

## How it works

1. Create a Vertebrae Server and attach Applications (APIs) to it with defined Vertebrae Routes.
2. (optionally) Supply connection details to one of the available databases. Vertebrae will create async connection pools to them.
3. Convert your classes to Vertebrae Services. This creates a mesh network between them. Each service gets a handler to your databases.

## Quick start

Copy the [sample](sample) application and use it as a template for your own projects.

## Advanced

### Vertebrae Core

The core module (vertebrae.core) contains the Server and Application classes, which are required to start any app
backed by this framework. Additionally, the core module contains the following functions:

- ```create_logger(name)```: use this to create a logger instance from anywhere in your own application. Note that Vertebrae services have default loggers already.
- ```strip_request(request)```: strip data off API request objects regardless of which method (GET/POST/PUT/DELETE) was called.

Here is an example that uses both. 

Imagine you have an API route that looks like this:

```python
def routes(self) -> [Route]:
    return [Route(method='POST', route='/account', handle=self._post_account)]
        
@allowed
async def _post_account(self, data: dict):
    pass
```

You can create the following ```allowed``` decorator to add authentication to this API route. In this case, we are checking if the token in the header 
matches the token in the Config module. Any data passed in the request (POST data, query parameters, etc) is passed into the handler via the ```data``` parameter. Otherwise, a 403 is returned to the caller.

```python
from functools import wraps
from aiohttp import web

from vertebrae.config import Config
from vertebrae.core import create_log, strip_request

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

### Databases

Vertebrae supports the following databases, which are accessible from any service class:

- relational (Postgres)
- cache (Redis)
- directory (local file system)
- s3 (AWS S3 service)

To enable any, ensure your Config module is injected with the appropriate connection details. 

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

### Detect 

Prelude is beta testing a new continuous security testing service called Detect. This service is currently configured
inside Vertebrae as an option - however it is not enabled (nor is it possible to enable at this time). A future version
of this framework will make Detect available.
