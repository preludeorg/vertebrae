import json
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

        def dict_merge(dct, merge_dct):
            for k, v in merge_dct.items():
                if k in dct and isinstance(dct[k], dict) and isinstance(merge_dct[k], dict):
                    dict_merge(dct[k], merge_dct[k])
                else:
                    dct[k] = merge_dct[k]

        def strip_yml(path):
            if path and os.path.isfile(path):
                with open(path, encoding='utf-8') as seed:
                    return next(yaml.load_all(seed, Loader=yaml.FullLoader))
            return ''

        env = lower_keys(strip_yml(env))
        os_keys = [k for k in list(os.environ.keys()) if k.lower() in env.keys()]
        os_env = {k: json.loads(os.getenv(k)) if isinstance(env[k.lower()], dict) else os.getenv(k) for k in os_keys}
        dict_merge(env, lower_keys(os_env))
        return env
