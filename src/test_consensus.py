
import sys
import os
import time
import subprocess
import json
import pytest

import node
from utils import value_and_tally


DIR = 'logs/'
NODE_CMD_PREFIX = ['python2', '-u', 'src/node.py']  # -u forces stdin/stdout/stderr to be unbuffered


def wrap_stdout(config):
    # TODO wrap stderr?
    sys.stdout = open(DIR + str(config.port) + '-' + str(os.getpid()) + '.out', 'w')
    print 'Test: running config', config.port
    node.run(config)


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
        msg = search_for_string(folder + fname, target)
        if msg is not None:
            msg = msg.replace(target, '').strip()
            print 'Test: found', f(msg)
            res.append(f(msg))
    return res


def run_subprocesses(prefix, cmds, outfs):
    assert len(cmds) == len(outfs)
    ps = []
    for cmd, outf in zip(cmds, outfs):
        print "Test: running subprocess", prefix + cmd
        with open(DIR + outf, 'wb') as fd:
            p = subprocess.Popen(prefix + cmd, stdout=fd)
            ps.append(p)
    return ps


@pytest.fixture
def discover():
    p = subprocess.Popen(['python2', 'src/discovery.py'])
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
    acs_msgs = search_for_string_in_dir(DIR, target, json.loads)

    # do various checks
    assert len(acs_msgs) >= n - t, "ACS incorrect length! len = {}, n = {}, t = {}".format(len(acs_msgs), n, t)

    s = acs_msgs[0]['set']
    ones = [x for x in s if x == 1]
    assert len(ones) >= n - t, "ACS not enough ones! s = {}".format(s)

    for msg in acs_msgs:
        assert s == msg['set'], "Test: ACS set mismatch! s = {}, msg = {}".format(s, msg['set'])

    m = acs_msgs[0]['msgs']
    for msg in acs_msgs:
        assert m == msg['msgs'], "Test: ACS msgs mismatch! m = {}, msg = {}".format(m, msg['msgs'])


def check_bracha_files(n, t):
    target = 'Bracha: DELIVER'
    bracha_msgs = search_for_string_in_dir(DIR, target)

    assert len(bracha_msgs) >= n - t, "Bracha incorrect length! len = {}, n = {}, t = {}".format(len(bracha_msgs), n, t)

    m, tally = value_and_tally(bracha_msgs).most_common(1)[0]
    assert tally >= n - t, "Bracha incorrect tally! tally = {}, n = {}, t = {}, m = {}".format(tally, n, t, m)


def check_mo14_files(n, t):
    # TODO check expected value v
    target = 'Mo14: DECIDED'
    mo14_msgs = search_for_string_in_dir(DIR, target)

    assert len(mo14_msgs) >= n - t, "Mo14 incorrect length! len = {}, n = {}, t = {}".format(len(mo14_msgs), n, t)

    vs = [int(x) for x in mo14_msgs]
    v, tally = value_and_tally(vs).most_common(1)[0]

    assert tally >= n - t, "Mo14 incorrect tally! tally = {}, n = {}, t = {}, v = {}".format(tally, n, t, v)


@pytest.mark.parametrize("n,t", [
    (4, 1),
    # (7, 2),
    (19, 6),
])
def test_acs(n, t, discover, folder):
    configs = []
    for i in range(n - t):
        configs.append(node.Config(12345 + i, n, t, test='acs'))
    for i in range(t):
        configs.append(node.Config(11111 + i, n, t, silent=True))

    ps = run_subprocesses(NODE_CMD_PREFIX,
                          [cfg.make_args() for cfg in configs],
                          [str(cfg.port) + '.out' for cfg in configs])

    time.sleep(30)
    for p in ps:
        p.terminate()

    # TODO not sure where to flush, so use sleep for now...
    time.sleep(1)
    print "Test: ACS nodes terminated"
    check_acs_files(n, t)
    print "Test: ACS test passed"


@pytest.mark.parametrize("n,t", [
    (4, 1),
    # (7, 2),
    (19, 6),
])
def test_bracha(n, t, discover, folder):
    configs = [node.Config(12345, n, t, test='bracha')]
    for i in range(n - t - 1):
        configs.append(node.Config(12345 + 1 + i, n, t))
    for i in range(t):
        configs.append(node.Config(11111 + i, n, t, silent=True))

    ps = run_subprocesses(NODE_CMD_PREFIX,
                          [cfg.make_args() for cfg in configs],
                          [str(cfg.port) + '.out' for cfg in configs])

    time.sleep(30)
    for p in ps:
        p.terminate()

    # TODO not sure where to flush, so use sleep for now...
    time.sleep(1)
    print "Test: Bracha nodes terminated"
    check_bracha_files(n, t)
    print "Test: Bracha test passed"


@pytest.mark.parametrize("n,t", [
    (4, 1),
    # (7, 2),
    (19, 6),
])
def test_mo14(n, t, discover, folder):
    configs = []
    for i in range(n - t):
        configs.append(node.Config(12345 + i, n, t, test='mo14'))
    for i in range(t):
        configs.append(node.Config(11111 + i, n, t, test='mo14', byzantine=True))

    ps = run_subprocesses(NODE_CMD_PREFIX,
                          [cfg.make_args() for cfg in configs],
                          [str(cfg.port) + '.out' for cfg in configs])

    time.sleep(30)
    for p in ps:
        p.terminate()

    time.sleep(1)
    print "Test: Mo14 nodes terminates"
    check_mo14_files(n, t)
    print "Test: Mo14 test passed"

