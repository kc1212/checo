
import sys
import os
import time
import node
import json
import pytest
from multiprocessing import Process


DIR = 'logs/'


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


def run_with_cfgs(cfgs):
    ps = []
    for cfg in cfgs:
        print "running", cfg.port
        p = Process(target=wrap_stdout, args=(cfg,))
        p.start()
        ps.append(p)
    return ps


@pytest.fixture
def discover():
    import discovery
    p = Process(target=discovery.run, args=())
    p.start()
    yield None
    print "Test: tear down discovery"
    p.terminate()


@pytest.fixture
def folder():
    if not os.path.exists(DIR):
        os.makedirs(DIR)

    delete_contents_of_dir(DIR)


def check_acs_files(n, t):
    """
    assume we have a clean directory
    :return: boolean
    """
    target = 'ACS: DONE'
    print os.listdir(DIR)
    acs_msgs = search_for_string_in_dir(DIR, target, json.loads)

    # do various checks
    if len(acs_msgs) < n - t:
        print "Test: ACS incorrect length!", len(acs_msgs), n, t
        return False

    s = acs_msgs[0]['set']
    ones = [x for x in s if x == 1]
    if len(ones) < n - t:
        print "Test: ACS not enough ones", s
        return False

    for msg in acs_msgs:
        if s != msg['set']:
            print "Test: ACS set mismatch!", s, msg['set']
            return False

    m = acs_msgs[0]['msgs']
    for msg in acs_msgs:
        if m != msg['msgs']:
            print "Test: ACS msgs mismatch!", m, msg['msgs']
            return False

    return True


def check_bracha_files(n, t):
    target = 'Bracha: DELIVER'
    bracha_msgs = search_for_string_in_dir(DIR, target)
    if len(bracha_msgs) < n - t:
        print "Test: Bracha incorrect length!", len(bracha_msgs), n, t
        return False

    # TODO it's more accurate to check that n - t delivered the same message rather than all
    m = bracha_msgs[0]
    for msg in bracha_msgs:
        if m != msg:
            print "Test: Bracha msgs mismatch!", m, msg
            return False

    return True


@pytest.mark.parametrize("n,t", [
    (4, 0),
    (7, 2),
])
def test_acs(n, t, discover, folder):
    configs = []
    for i in range(n - t):
        configs.append(node.Config(12345 + i, n, t, test='acs'))
    for i in range(t):
        configs.append(node.Config(11111 + i, n, t, silent=True))

    ps = run_with_cfgs(configs)

    time.sleep(30)
    for p in ps:
        p.terminate()

    # TODO not sure where to flush, so use sleep for now...
    time.sleep(5)
    print "Test: ACS nodes terminated"
    assert check_acs_files(n, t)
    print "Test: ACS test passed"


@pytest.mark.parametrize("n,t", [
    (4, 0),
    (7, 2),
])
def test_bracha(n, t, discover, folder):
    configs = []
    configs.append(node.Config(12345, n, t, test='bracha'))
    for i in range(n - t - 1):
        configs.append(node.Config(12345 + 1 + i, n, t))
    for i in range(t):
        configs.append(node.Config(11111 + i, n, t, silent=True))

    ps = run_with_cfgs(configs)

    time.sleep(30)
    for p in ps:
        p.terminate()

    time.sleep(5)
    print "Test: Bracha nodes terminated"
    assert check_bracha_files(n, t)
    print "Test: Bracha test passed"


