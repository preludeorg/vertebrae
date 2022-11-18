import os

import yaml


class Config:
    """ All configuration details live here """

    _configs = dict()

    @classmethod
    def load(cls, config):
        """ Set a new config database """
        cls._configs = config

    @classmethod
    def find(cls, prop=None, fallback=None):
        """ Find a property """
        return cls._configs.get(prop.lower(), fallback)

    @staticmethod
    def strip(env):
        """ Inject config properties from env variables """
        def lower_keys(dct):
            if isinstance(dct, dict):
                return {k.lower(): lower_keys(v) for k, v in dct.items()}
            return dct

        def strip_yml(path):
            if path and os.path.isfile(path):
                with open(path, encoding='utf-8') as seed:
                    return next(yaml.load_all(seed, Loader=yaml.FullLoader))
            return ''

        env = lower_keys(strip_yml(env))
        for k in os.environ.keys():
            formated = k.lower().replace("_",".")
            if formated in env.keys():
                env[formated] = os.environ[k]

        return env
