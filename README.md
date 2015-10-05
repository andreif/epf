# EPF parser

```py
import epf.parser

for table in epf.parser.parse('path/to/file.tbz'):
    print(table)
    column_names = [c[0] for c in table['columns']]
    for r in table['record_generator']():
        print(dict(zip(column_names, r)))
```
