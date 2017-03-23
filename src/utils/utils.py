import json
import logging
import sys
from collections import Counter


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


def value_and_tally(xs):
    """
    Given a list, get the unique values and their respective tally.
    :param xs:
    :return: Counter
    """
    res = Counter()
    for x in xs:
        res[x] += 1

    return res.most_common(1)[0]


class BColors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class JsonSerialisable:
    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__)


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


def make_args(port, n, t, test=None, value=0, failure=None, tx=0, loglevel=logging.INFO, output=None):
    res = [str(port), str(n), str(t)]

    if test is not None:
        res.append('--test')
        res.append(test)

    res.append('--value')
    res.append(str(value))

    if failure is not None:
        res.append('--failure')
        res.append(failure)

    res.append('--tx')
    res.append(str(tx))

    if loglevel == logging.DEBUG:
        res.append('--debug')
    elif loglevel == logging.INFO:
        res.append('-v')

    # None represents stdout
    if output is not None:
        res.append('-o')
        res.append(output)

    return res


