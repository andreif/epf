# EPF crawler, downloader and parser

```
pip install epf
```

### Parser

```py
import epf.parser

for table in epf.parser.parse('path/to/file.tbz'):
    print(table)
    column_names = [c[0] for c in table['columns']]
    for r in table['record_generator']():
        print(dict(zip(column_names, r)))
```

```py
table = epf.parser.parse('path/to/table_file').next():
print(table)
column_names = [c[0] for c in table['columns']]
for r in table['record_generator']():
    print(dict(zip(column_names, r)))
```

### Crawler

```py
import epf.crawler
import re

crawl = lambda u, r, f=None, d=False: \
    epf.crawler.crawl(url=u, auth=('user', 'pass'), recursive=r, filter_=f,
                      yield_dirs=d)


url = sorted(crawl(EPF_V4_FULL_URL, r=False, d=True))[-1]
assert re.match('^.*/\d{8}/$', url)

rx = re.compile('.*(itunes|incremental|application|popularity).*|^\d+/$')
f = lambda n: rx.match(n)

for l in crawl(url, r=True, f=f):
    print(l)
```

### Downloader

```py
import epf.downloader

epf.downloader.download(tbz_url, tbz_path, auth=('user', 'pass'))
```

License: MIT
