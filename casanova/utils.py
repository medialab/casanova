# =============================================================================
# Casanova Utils
# =============================================================================
#
# Miscellaneous utility functions.
#
import re
import gzip
import sys
from io import BytesIO, BufferedReader, TextIOWrapper
from ebbe import with_prev


def is_contiguous(l):
    for p, n in with_prev(l):
        if p is None:
            continue

        if p != n - 1:
            return False

    return True


def is_binary_buffer(buf):
    if isinstance(buf, gzip.GzipFile) and not isinstance(buf, TextIOWrapper):
        return True
    if isinstance(buf, BufferedReader):
        if 'b' not in buf.mode:
            return False
    elif not isinstance(buf, BytesIO):
        return False

    return True


def is_resumable_buffer(buf):
    if not isinstance(buf, (BufferedReader, TextIOWrapper)):
        return False

    if 'a' not in buf.mode and '+' not in buf.mode:
        return False

    return True


def is_empty_buffer(buf):
    return buf.tell() == 0


def is_mute_buffer(buf):
    return buf is sys.stdout or buf is sys.stderr or not hasattr(buf, 'tell')


def encoding_fingerprint(encoding):
    return encoding.lower().replace('-', '')


def ensure_open(p, encoding='utf-8', mode='r'):
    if not isinstance(p, str):
        return p

    if p.endswith('.gz'):
        if 'b' in mode:
            return gzip.open(p, mode=mode)

        mode += 't'
        return gzip.open(p, encoding=encoding, mode=mode)

    if encoding_fingerprint(encoding) != 'utf8':
        return codecs.open(p, encoding=encoding, mode=mode)

    return open(p, mode=mode)


BOM_RE = re.compile(r'^\ufeff')


def suppress_BOM(string):
    return re.sub(BOM_RE, '', string)


def count_bytes_in_row(row):
    return sum(len(cell) for cell in row) * 2
