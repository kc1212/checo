import os
import re
import subprocess
import time
import logging
import pytest
from collections import Counter

GOOD_PORT = 30000
BAD_PORT = 10000
DIR = 'logs/'
NODE_CMD_PREFIX = ['python2', '-u', '-m', 'src.node']  # -u forces stdin/stdout/stderr to be unbuffered


@pytest.fixture
def discover():
    p = subprocess.Popen(['python2', '-m', 'src.discovery'])
    time.sleep(1)  # wait for it to spin up
    yield None
    print "Test: tear down discovery"
    p.terminate()


@pytest.fixture
def folder():
    if not os.path.exists(DIR):
        os.makedirs(DIR)

    delete_contents_of_dir(DIR)


def poll_check_f(to, tick, ps, f, *args, **kwargs):
    """
    Runs f with parameters *args and **kwargs once every `tick` seconds and time out at `to`
    :param to: timeout
    :param tick: clock tick
    :param ps: processes to terminate upon completion
    :param f:
    :param args:
    :param kwargs:
    :return:
    """
    def terminate_ps(_ps):
        for _p in _ps:
            _p.terminate()

    while to > 0:
        to -= tick
        time.sleep(tick)
        try:
            f(*args, **kwargs)
            terminate_ps(ps)
            return
        except AssertionError as e:
            print "poll not ready", e

    terminate_ps(ps)
    f(*args, **kwargs)


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


def delete_contents_of_dir(dname):
    for f in os.listdir(dname):
        fpath = os.path.join(dname, f)
        if os.path.isfile(fpath):
            os.unlink(fpath)


def search_for_string(fname, target):
    with open(fname, 'r') as f:
        for line in f:
            if target in line:
                print "Test: found target in line", target, line
                return line
    print "Test: did not find", target, "in file", fname
    return None


def search_for_string_in_dir(dir, target, f=lambda x: x):
    res = []
    for fname in os.listdir(dir):

        # we only care about output of honest nodes
        if not re.match("^3.*\.out$", fname):
            continue

        msg = search_for_string(dir + fname, target)
        if msg is not None:
            msg = msg.split(target)[-1].strip()
            print 'Test: found', f(msg)
            res.append(f(msg))
    return res


def run_subprocesses(prefix, cmds, sleep_interval=0):
    ps = []
    for cmd in cmds:
        print "Test: running subprocess", prefix + cmd
        p = subprocess.Popen(prefix + cmd)
        ps.append(p)
        if sleep_interval != 0:
            time.sleep(sleep_interval)
    return ps


def make_args(port, n, t, test=None, value=0, failure=None, tx=0, loglevel=logging.DEBUG, output=None,
              broadcast=True):
    """
    This function should produce all the parameters accepted by argparse
    :param port:
    :param n:
    :param t:
    :param test:
    :param value:
    :param failure:
    :param tx:
    :param loglevel:
    :param output:
    :param broadcast:
    :return:
    """
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

    if broadcast:
        res.append('--broadcast')

    return res

