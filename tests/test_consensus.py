import re
import json
import pickle
import subprocess
import time
import random

import os
import pytest

from src import node
from src.utils.utils import value_and_tally

GOOD_PORT = 30000
BAD_PORT = 10000
DIR = 'logs/'
NODE_CMD_PREFIX = ['python2', '-u', '-m', 'src.node']  # -u forces stdin/stdout/stderr to be unbuffered
# NODE_CMD_PREFIX = ['python2', '-u', '-m', 'src.node', '--debug']


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


def search_for_string_in_dir(folder, target, f=lambda x: x):
    res = []
    for fname in os.listdir(folder):

        # we only care about output of honest nodes
        if not re.match("^3.*\.out$", fname):
            continue

        msg = search_for_string(folder + fname, target)
        if msg is not None:
            msg = msg.split(target)[-1].strip()
            print 'Test: found', f(msg)
            res.append(f(msg))
    return res


def run_subprocesses(prefix, cmds):
    ps = []
    for cmd in cmds:
        print "Test: running subprocess", prefix + cmd
        p = subprocess.Popen(prefix + cmd)
        ps.append(p)
    return ps


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


def check_acs_files(n, t):
    target = 'ACS: DONE'
    print os.listdir(DIR)
    acs_dones = search_for_string_in_dir(DIR, target, json.loads)

    # do various checks
    assert len(acs_dones) >= n - t, "ACS incorrect length! len = {}, n = {}, t = {}".format(len(acs_dones), n, t)

    # first find the agreed set and check there's majority
    # NOTE: we can't use value_and_tally because dictionary is unhashable
    s = acs_dones[0]['set']
    tally_s = 0
    for x in acs_dones:
        if s == x['set']:
            tally_s += 1
    assert tally_s >= n - t

    # filter the messages that is not in the agreed set
    key_of_ones = [k for k, v in s.iteritems() if v == 1]
    print "key of ones", key_of_ones
    assert len(key_of_ones) >= n - t

    # NOTE: we manually do this too because dictionary is unhashable
    msgs = {k: acs_dones[0]['msgs'][k] for k in key_of_ones}
    tally_msgs = 0
    for x in acs_dones:
        if msgs == {k: x['msgs'][k] for k in key_of_ones}:
            tally_msgs += 1
    assert tally_msgs >= n - t


def check_bracha_files(n, t):
    target = 'Bracha: DELIVER'
    bracha_delivers = search_for_string_in_dir(DIR, target)

    assert len(bracha_delivers) >= n - t, "Bracha incorrect length! len = {}, n = {}, t = {}".format(len(bracha_delivers), n, t)

    m, tally = value_and_tally(bracha_delivers)
    assert tally >= n - t, "Bracha incorrect tally! tally = {}, n = {}, t = {}, m = {}".format(tally, n, t, m)


def check_mo14_files(n, t, expected_v):
    target = 'Mo14: DECIDED'
    mo14_decides = search_for_string_in_dir(DIR, target)

    assert len(mo14_decides) >= n - t, "Mo14 incorrect length! len = {}, n = {}, t = {}".format(len(mo14_decides), n, t)

    vs = [int(x) for x in mo14_decides]
    v, tally = value_and_tally(vs)

    assert tally >= n - t, "Mo14 incorrect tally! tally = {}, n = {}, t = {}, v = {}".format(tally, n, t, v)
    assert int(v) == expected_v


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

    f(*args, **kwargs)
    terminate_ps(ps)


@pytest.mark.parametrize("n,t,f", [
    (4, 1, 'omission'),
    (7, 2, 'omission'),
    (19, 6, 'omission'),
    (4, 1, 'byzantine'),
    (7, 2, 'byzantine'),
    (19, 6, 'byzantine'),
])
def test_acs(n, t, f, folder, discover):
    configs = []
    for i in range(n - t):
        port = GOOD_PORT + i
        configs.append(node.Config(port, n, t, test='acs', output=DIR + str(port) + '.out'))
    for i in range(t):
        port = BAD_PORT + i
        configs.append(node.Config(port, n, t, test='acs', failure=f, output=DIR + str(port) + '.out'))

    ps = run_subprocesses(NODE_CMD_PREFIX, [cfg.make_args() for cfg in configs])

    print "Test: ACS polling"
    poll_check_f(120, 5, ps, check_acs_files, n, t)
    print "Test: ACS test passed"


@pytest.mark.parametrize("n,t,f", [
    (4, 1, 'omission'),
    (7, 2, 'omission'),
    (19, 6, 'omission'),
])
def test_bracha(n, t, f, folder, discover):
    configs = [node.Config(GOOD_PORT, n, t, test='bracha', output=DIR + str(GOOD_PORT) + '.out')]
    for i in range(n - t - 1):
        port = GOOD_PORT + 1 + i
        configs.append(node.Config(port, n, t, output=DIR + str(port) + '.out'))
    for i in range(t):
        port = BAD_PORT + i
        configs.append(node.Config(port, n, t, failure=f, output=DIR + str(port) + '.out'))

    ps = run_subprocesses(NODE_CMD_PREFIX, [cfg.make_args() for cfg in configs])

    print "Test: Bracha polling"
    poll_check_f(20, 5, ps, check_bracha_files, n, t)
    print "Test: Bracha test passed"


@pytest.mark.parametrize("n,t,f", [
    (4, 1, 'byzantine'),
    (7, 2, 'byzantine'),
    (19, 6, 'byzantine'),
    (4, 1, 'omission'),
    (7, 2, 'omission'),
    (19, 6, 'omission'),
])
def test_mo14(n, t, f, folder, discover):
    v = random.randint(0, 1)
    configs = []
    for i in range(n - t):
        port = GOOD_PORT + i
        configs.append(node.Config(port, n, t, test='mo14', value=v, output=DIR + str(port) + '.out'))
    for i in range(t):
        port = BAD_PORT + i
        randv = random.randint(0, 1)
        configs.append(node.Config(port, n, t, test='mo14', value=randv, failure=f, output=DIR + str(port) + '.out'))

    ps = run_subprocesses(NODE_CMD_PREFIX, [cfg.make_args() for cfg in configs])

    print "Test: Mo14 polling"
    poll_check_f(20, 5, ps, check_mo14_files, n, t, v)
    print "Test: Mo14 test passed"


if __name__ == '__main__':
    check_acs_files(19, 6)
