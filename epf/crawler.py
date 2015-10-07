import logging
import re
import requests

log = logging.getLogger(__name__)


def crawl(url, auth, recursive=True, filter_=None, yield_dirs=False):
    log.debug('Fetching %s...', url)
    r = requests.get(url, auth=auth)
    assert r.ok and '<th><a href="?C=N;O=D">Name</a></th>' in r.text

    for m in re.findall('href="([^"\?]+(/|\.tbz))"', r.text, flags=re.MULTILINE):
        m = m[0]
        if filter_ and not filter_(m):
            continue
        is_dir = m.endswith('/')
        if not is_dir or yield_dirs:
            yield url + m
        if recursive and is_dir:
            yield from crawl(url + m, auth=auth, recursive=recursive,
                             filter_=filter_, yield_dirs=yield_dirs)
