import os
from six.moves.configparser import Error as ScrapyCfgParseError
from six.moves.configparser import ConfigParser
from subprocess import CalledProcessError
from shutil import copyfile, rmtree
from subprocess import check_call
import sys
import glob
import tempfile
import time


_SETUP_PY_TEMPLATE = """# Automatically created by: scrapydweb x scrapyd-client
from setuptools import setup, find_packages
setup(
    name         = 'project',
    version      = '1.0',
    packages     = find_packages(),
    entry_points = {'scrapy': ['settings = %(settings)s']},
)
"""


def get_config(sources):
    """Get Scrapy config file as a ConfigParser"""
    # sources = get_sources(use_closest)
    cfg = ConfigParser()
    cfg.read(sources)
    return cfg


def _create_default_setup_py(**kwargs):
    with open('setup.py', 'w') as f:
        content = _SETUP_PY_TEMPLATE % kwargs
        f.write(content)


def retry_on_eintr(func, *args, **kw):
    """Run a function and retry it while getting EINTR errors"""
    while True:
        try:
            return func(*args, **kw)
        except IOError as e:
            if e.errno != errno.EINTR:
                raise


def _build_egg(scrapy_config_path):
    cwd = os.getcwd()  # current working directory
    try:
        os.chdir(os.path.dirname(scrapy_config_path))
        settings = get_config(scrapy_config_path).get('settings', 'default')
        _create_default_setup_py(settings=settings)
        d = tempfile.mkdtemp(prefix="scrapydweb-deploy-")
        o = open(os.path.join(d, "stdout"), "wb")
        e = open(os.path.join(d, "stderr"), "wb")
        retry_on_eintr(check_call, [sys.executable, 'setup.py', 'clean', '-a', 'bdist_egg', '-d', d],
                       stdout=o, stderr=e)
        egg = glob.glob(os.path.join(d, '*.egg'))[0]
        o.close()
        e.close()

    except:
        os.chdir(cwd)
        raise
    finally:
        os.chdir(cwd)

    return egg, d


def build_egg(scrapy_config_path, eggname):
    try:
        egg, tmpdir = _build_egg(scrapy_config_path)
    except ScrapyCfgParseError as err:
            print(err)
            return
    except CalledProcessError as err:
        print(err)

    copyfile(egg, os.path.join(os.path.dirname(__file__), eggname))
    rmtree(tmpdir)


if __name__ == "__main__":
    pr = os.getenv('PR_NUMBER', None)

    project_path = os.path.join(os.pardir, os.pardir, "mensa-scraper", "rnr")
    scrapy_config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), project_path, "scrapy.cfg"))
    eggname = '%s-%s.egg' % ("rnr", pr) if pr else 'rnr.egg'
    build_egg(scrapy_config_path, eggname)

    project_path = os.path.join(os.pardir, os.pardir, "mensa-scraper", "brokenness")
    scrapy_config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), project_path, "scrapy.cfg"))
    eggname = '%s-%s.egg' % ("brokenness", pr) if pr else 'brokenness.egg'
    build_egg(scrapy_config_path, eggname)

    project_path = os.path.join(os.pardir, os.pardir, "mensa-scraper", "hotstyles")
    scrapy_config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), project_path, "scrapy.cfg"))
    eggname = '%s-%s.egg' % ("hotstyles", pr) if pr else 'hotstyles.egg'
    build_egg(scrapy_config_path, eggname)

    project_path = os.path.join(os.pardir, os.pardir, "mensa-scraper", "discoverability")
    scrapy_config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), project_path, "scrapy.cfg"))
    eggname = '%s-%s.egg' % ("discoverability", pr) if pr else 'discoverability.egg'
    build_egg(scrapy_config_path, eggname)

    project_path = os.path.join(os.pardir, os.pardir, "mensa-scraper", "serviceability")
    scrapy_config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), project_path, "scrapy.cfg"))
    eggname = '%s-%s.egg' % ("serviceability", pr) if pr else 'serviceability.egg'
    build_egg(scrapy_config_path, eggname)

    project_path = os.path.join(os.pardir, os.pardir, "mensa-scraper", "bestsellers")
    scrapy_config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), project_path, "scrapy.cfg"))
    eggname = '%s-%s.egg' % ("bestsellers", pr) if pr else 'bestsellers.egg'
    build_egg(scrapy_config_path, eggname)

    project_path = os.path.join(os.pardir, os.pardir, "mensa-scraper", "pricing")
    scrapy_config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), project_path, "scrapy.cfg"))
    eggname = '%s-%s.egg' % ("pricing", pr) if pr else 'pricing.egg'
    build_egg(scrapy_config_path, eggname)