import os

basedir = os.path.abspath(os.path.dirname(__file__))


class BaseConfig(object):
    """Base configuration."""
    WTF_CSRF_ENABLED = True
    REDIS_URL = 'redis://redis:6379/0'
    # QUEUES = ['bso-publications', 'unpaywall_to_crawler', 'zotero', 'scanr-publications']
    # QUEUES = ['bso-publications', 'unpaywall_to_crawler', 'zotero']
    QUEUES = ['scanr-publications', 'unpaywall_to_crawler', 'zotero']


class DevelopmentConfig(BaseConfig):
    """Development configuration."""
    WTF_CSRF_ENABLED = False


class TestingConfig(BaseConfig):
    """Testing configuration."""
    TESTING = True
    WTF_CSRF_ENABLED = False
    PRESERVE_CONTEXT_ON_EXCEPTION = False
