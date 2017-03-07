
import sys
import os
import time
import node
import json
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


def check_acs_files(n, t):
    """
    assume we have a clean directory
    :return: boolean
    """
    acs_msgs = []
    target = 'ACS: DONE'
    print os.listdir(DIR)
    for fname in os.listdir(DIR):
        msg = search_for_string(DIR + fname, target)
        if msg is not None:
            msg = msg.replace(target, '').strip()
            print 'Test: found ACS DONE msg', json.loads(msg)
            acs_msgs.append(json.loads(msg))

    # do various checks
    if len(acs_msgs) < n - t:
        print "Test: incorrect length!", len(acs_msgs), n, t
        return False

    s = acs_msgs[0]['set']
    ones = [x for x in s if x == 1]
    if len(ones) < n - t:
        print "Test: not enough ones", s
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

    # TODO test msgs
    return True


def test_simple_acs():
    n = 4
    t = 1
    configs = [
        node.Config(12345, n, t, test='acs'),
        node.Config(12346, n, t, test='acs'),
        node.Config(12347, n, t, test='acs'),
        node.Config(12348, n, t, silent=True)
    ]

    # TODO put this in the setup phase
    if not os.path.exists(DIR):
        os.makedirs(DIR)

    delete_contents_of_dir(DIR)

    ps = []
    for cfg in configs:
        print "running", cfg.port
        p = Process(target=wrap_stdout, args=(cfg,))
        p.start()
        ps.append(p)

    time.sleep(30)
    for p in ps:
        p.terminate()

    # TODO not sure where to flush, so use sleep for now...
    time.sleep(5)
    print "Test: nodes terminated"
    assert check_acs_files(n, t)
    print "Test: ACS test passed"

if __name__ == '__main__':
    test_simple_acs()

