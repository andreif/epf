import codecs
import io
import json
import logging
import os
import shutil
import datetime
import requests
import time
from .ctx import DelayedKeyboardInterrupt

log = logging.getLogger(__name__)


def szf(n):
    units = 'TGMK'
    for i, u in enumerate(units):
        x = 1024 ** (len(units) - i)
        if n >= x * 999.0 / 1024:
            break
    else:
        x = 1
        u = ''
    return '%.3g%sB' % (float(n)/x, u)


def tmf(n):
    s = int(n % 60)
    m = int((n / 60) % 60)
    h = int((n / 60 / 60) % 24)
    d = int((n / 60 / 60 / 24))
    t = ''
    if d > 0:
        t = '%dd' % d
    if h > 0:
        t += '%dh' % h
    if m > 0:
        t += '%dm' % m
    t += '%ds' % s
    return t


def check_md5(path):
    log.debug('Checking md5 of %s', path)
    r = os.system(
        "grep $(openssl md5 {0} | awk '{{print $2}}') {0}.md5 1>/dev/null"
        .format(path)
    )
    if r:
        log.error('Wrong checksum')
        os.system('openssl md5 {0}; cat {0}.md5'.format(path))
    return r == 0


def download(url, save_to, auth, check_downloaded=False):
    chunk_size = 1024 * 1024 * 1
    retries = 5
    retry_wait = 3
    log_interval = 3
    download_path = save_to
    headers_path = download_path + '.headers'
    partial_path = download_path + '.part'
    md5_path = download_path + '.md5'

    log.debug('Downloading file from: %s', url)
    log.debug('...to: %s', download_path)

    while True:

        if not check_downloaded and os.path.exists(download_path):
            log.debug('Already downloaded, not checking')
            return

        log.debug('Requesting file headers...')
        r = requests.head(url, auth=auth)
        assert r.ok

        total_size = int(r.headers['content-length'])
        assert r.headers['accept-ranges'] == 'bytes'

        if os.path.exists(headers_path):
            with open(headers_path) as f:
                h = json.loads(f.read())
            if h['etag'] != r.headers['etag']:
                log.error('Warning: remote file changed: %r, %r', h, r.headers)
                log.debug('Deleting all local files')
                os.unlink(headers_path)

                if os.path.exists(partial_path):
                    os.unlink(partial_path)

                if os.path.exists(download_path):
                    os.unlink(download_path)

        if not os.path.exists(headers_path):
            log.debug('Saving header file...')
            with DelayedKeyboardInterrupt():
                with open(headers_path, 'w+') as f:
                    d = dict(r.headers.items())
                    d['url'] = url
                    f.write(json.dumps(d, indent=2))

        if not os.path.exists(md5_path):
            log.debug('Fetching md5 file...')
            r = requests.get(url + '.md5', auth=auth)
            assert r.ok and len(r.text)
            with DelayedKeyboardInterrupt():
                with open(md5_path, 'w+') as f:
                    f.write(r.text)
        else:
            log.debug('Checking md5 file...')
            assert not os.system('grep tbz %s 1>/dev/null' % md5_path)

        if check_downloaded and os.path.exists(download_path):
            offset = os.path.getsize(download_path)

            log.debug('Already downloaded, checking size and md5...')
            if offset == total_size and check_md5(download_path):
                log.debug('Correct file.')
                return
            else:
                log.debug('Wrong file, deleting downloaded')
                os.unlink(download_path)

        if os.path.exists(partial_path):
            offset = os.path.getsize(partial_path)
            if offset == total_size:
                log.debug('Partial file seems complete, renaming')
                with DelayedKeyboardInterrupt():
                    shutil.move(partial_path, download_path)
                if check_md5(download_path):
                    return
                else:
                    os.unlink(download_path)
                    offset = 0
        else:
            offset = 0

        rates_history = []

        def log_progress(prev_offset=None, dt=None):
            remain = total_size - offset
            s = 'Progress: %6s / %6s = %5.1f%%; remaining: %6s' % (
                szf(offset), szf(total_size), float(offset) / total_size * 100,
                szf(remain))

            if dt and prev_offset:
                rate = float(offset - prev_offset) / dt
                rates_history.insert(0, rate)
                avg_rate = sum(rates_history[:10]) / len(rates_history[:10])
                t = float(remain) / avg_rate
                d = datetime.datetime.now() + datetime.timedelta(seconds=t)

                s += '; rate = %6s/s' % szf(rate)
                s += '; avg.rate = %6s/s, eta = %-8s (%s)' % (
                    szf(avg_rate), tmf(t),
                    d.strftime('%H:%M')
                )
            log.debug(s)
        log_progress()

        if offset > total_size:
            log.error('Error: offset > total size. Deleting part file')
            os.unlink(partial_path)
            offset = 0
            log_progress()

        inset = int(min(offset, 10))
        inset_checked = False
        inset_value = ''

        range_spec = '{a}-{b}'.format(a=offset - inset, b=total_size - 1)

        t = time.time()
        log.debug('Fetching range %s', range_spec)

        r = requests.get(url, stream=True, auth=auth,
                         headers={'Range': 'bytes=' + range_spec})
        if r.ok:
            assert r.headers['content-range'] == \
                'bytes %s/%s' % (range_spec, total_size)

            # TODO: check range, set range_failed if not true and raise;
            # TODO: if range already failed, overwrite files?

            prev_offset = offset - inset

            if inset:
                with open(partial_path, 'rb') as f:
                    f.seek(-inset, io.SEEK_END)
                    inset_value = f.read(inset)
                    log.debug('Expected inset %s-%s: %s', offset - inset,
                              offset, codecs.encode(inset_value, 'hex')
                              .decode('utf8'))
                    assert len(inset_value) == inset

            with open(partial_path, 'ab+') as f:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if chunk:  # filter out keep-alive new chunks
                        if inset and not inset_checked:
                            log.debug('Checking inset: %s',
                                      codecs.encode(chunk[:inset], 'hex')
                                      .decode('utf8'))
                            assert inset_value == chunk[:inset]
                            chunk = chunk[inset:]
                            inset_checked = True

                        offset += len(chunk)
                        with DelayedKeyboardInterrupt():
                            f.write(chunk)
                            f.flush()

                        dt = time.time() - t
                        if dt > log_interval:
                            log_progress(prev_offset=prev_offset, dt=dt)
                            t = time.time()
                            prev_offset = offset

            log_progress(prev_offset, time.time() - t)

            offset = os.path.getsize(partial_path)
            if offset == total_size:
                with DelayedKeyboardInterrupt():
                    shutil.move(partial_path, download_path)
                if not check_md5(download_path):
                    os.unlink(download_path)
                    continue

        else:
            log.error('Failed: %s %r', r.status_code, r.headers)
            retries -= 1
            if retries <= 0:
                break

            time.sleep(retry_wait)
