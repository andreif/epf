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
        ln = ln.decode('utf8').replace('\x00', '')
        if ignore_comments and not lines and ln.startswith('#'):
            continue
        lines.append(ln)
        if record_delim in ln:
            lines[-1] = ln.partition(record_delim)[0]
            break
    return ''.join(lines) if lines else None


def repair_record(file_name, column_names, record):
    if os.path.basename(file_name) == 'application':
        if column_names[13] == 'description' and len(column_names) == 17:
            record = record[:13] + [''.join(record[13:-3])] + record[-3:]
        else:
            log.error('Unable to fix bad application description')
    return record


def parse(path):
    log.debug('Opening %s...', path)
    if path.endswith('.tbz'):
        with tarfile.open(path, 'r:bz2') as tar:
            for member in tar:
                if not member.isfile():
                    continue
                log.debug('Parsing %s...', member.name)
                with tar.extractfile(member) as f:
                    yield from parse_file(f, name=member.name)
    else:
        with open(path, 'rb') as f:
            yield from parse_file(f, name=os.path.basename(path))


def parse_file(f, name):
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

            if len(r) > len(column_names):
                r = repair_record(file_name=name, column_names=column_names,
                                  record=r)

            assert len(r) == len(column_names), \
                '%d %d %r %r' % (len(r), len(column_names), r, column_names)
            yield r

    yield {
        'file_name': name,
        'records_expected': records_expected,
        'columns': list(zip(column_names, headers['dbTypes'])),
        'record_generator': record_gen,
        'primary_keys': [k for k in headers['primaryKey'] if k],
        'is_incremental': headers['exportMode'][0] == 'INCREMENTAL',
    }
