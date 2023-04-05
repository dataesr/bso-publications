import os

basedir = os.path.abspath(os.path.dirname(__file__))

from bso import __version__

class BaseConfig(object):
    """Base configuration."""
    WTF_CSRF_ENABLED = True
    REDIS_URL = 'redis://redis:6379/0'
    QUEUES = ['bso-publications', 'unpaywall_to_crawler', 'zotero']
    if 'scanr' in __version__:
        QUEUES = ['scanr-publications', 'unpaywall_to_crawler', 'zotero']
    print('Queues = '+str(QUEUES), flush=True)


class DevelopmentConfig(BaseConfig):
    """Development configuration."""
    WTF_CSRF_ENABLED = False


class TestingConfig(BaseConfig):
    """Testing configuration."""
    TESTING = True
    WTF_CSRF_ENABLED = False
    PRESERVE_CONTEXT_ON_EXCEPTION = False
