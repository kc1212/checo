from twisted.internet import reactor, task

import jsonpickle
import logging
import sys
import libnacl

MAX_LINE_LEN = 1024 * 1024 * 1024


def byteify(inp):
    """
    Recursively encode an object from unicode into UTF-8, any object that is not of instance unicode is ignored.
    :param inp: The input object.
    :return: The encoded object.
    """
    if isinstance(inp, dict):
        return {byteify(key): byteify(value) for key, value in inp.iteritems()}
    elif isinstance(inp, list):
        return [byteify(element) for element in inp]
    elif isinstance(inp, unicode):
        return inp.encode('utf-8')
    else:
        return inp


def intersperce(iterable, delimiter):
    it = iter(iterable)
    yield next(it)
    for x in it:
        yield delimiter
        yield x


class Replay:
    """
    Dummy class returned by consensus algorithms to identify that the message should be replayed at a later time
    because it cannot yet be handled.
    """
    def __init__(self):
        pass


class Handled:
    """
    The Result type is the result of handling a message in the consensus algorithm
    """
    def __init__(self, m=None):
        self.m = m


def set_logging(lvl, stream=sys.stdout):
    logging.basicConfig(stream=stream, level=lvl, format='%(asctime)s - %(levelname)s - %(message)s')


def dictionary_hash(d):
    digest = ''
    for key in sorted(d):
        digest = libnacl.crypto_hash_sha256(digest + jsonpickle.encode(key) + jsonpickle.encode(d[key]))
    return digest


def flatten(l):
    return [item for sublist in l for item in sublist]


def collate_cp_blocks(d):
    res = []
    for key in sorted(d):
        res.append(d[key])
    return list(set(flatten(res)))


def call_later(delay, f, *args, **kw):
    task.deferLater(reactor, delay, f, *args, **kw).addErrback(my_err_back)


def hash_pointers_ok(blocks):
    prev = blocks[0].hash
    for b in blocks[1:]:
        if b.prev != prev:
            return False
        prev = b.hash
    return True


def my_err_back(failure):
    logging.error(failure.getErrorMessage())
    logging.error(failure.getTraceback())
    failure.printTraceback()
    reactor.stop()

