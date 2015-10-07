import logging
import os
import tarfile

log = logging.getLogger(__name__)

record_delim = '\x02\n'
field_delim = '\x01'


def read_record(f, ignore_comments=True):
    lines = []
    while True:
        ln = f.readline()
        if not ln:
            break
        ln = ln.decode('utf8')
        if ignore_comments and not lines and ln.startswith('#'):
            continue
        lines.append(ln)
        if record_delim in ln:
            lines[-1] = ln.partition(record_delim)[0]
            break
    return ''.join(lines) if lines else None


def parse(tbz_path):
    log.debug('Opening %s...', tbz_path)
    with tarfile.open(tbz_path, 'r:bz2') as tar:
        for member in tar:
            if not member.isfile():
                continue
            log.debug('Parsing %s...', member.name)
            with tar.extractfile(member) as f:
                f.seek(-40, os.SEEK_END)

                records_expected = int(
                    f.read().decode('utf8')
                    .rpartition('#recordsWritten:')[2]
                    .rpartition(record_delim)[0]
                )
                log.debug('Records expected: %s', records_expected)

                f.seek(0)

                r = read_record(f, ignore_comments=False)
                assert r.startswith('#')
                column_names = r[1:].split(field_delim)
                log.debug('Columns: %s', ', '.join(column_names))

                headers = {}
                for j in range(6):
                    r = read_record(f, ignore_comments=False)
                    if not r or r.startswith('##legal:'):
                        continue
                    assert r.startswith('#') and ':' in r, repr(r)
                    k, _, v = r[1:].partition(':')
                    headers[k] = v.split(field_delim)

                def record_gen():
                    f.seek(0)
                    while True:
                        r = read_record(f, ignore_comments=True)
                        if not r:
                            break
                        r = r.split(field_delim)
                        assert len(r) == len(column_names)
                        yield r

                yield {
                    'file_name': member.name,
                    'records_expected': records_expected,
                    'columns': list(zip(column_names, headers['dbTypes'])),
                    'record_generator': record_gen,
                    'primary_keys': [k for k in headers['primaryKey'] if k],
                    'is_incremental':
                        headers['exportMode'][0] == 'INCREMENTAL',
                }
