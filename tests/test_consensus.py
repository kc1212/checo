import json
import random

from tools import *


def check_acs_files(n, t):
    target = 'ACS: DONE'
    print os.listdir(DIR)
    acs_dones = search_for_string_in_dir(DIR, target, json.loads)

    # do various checks
    assert len(acs_dones) >= n - t, "ACS incorrect length! len = {}, n = {}, t = {}".format(len(acs_dones), n, t)

    m, tally = value_and_tally(acs_dones)
    assert tally >= n - t, "ACS incorrect tally! tally = {}, n = {}, t = {}, m = {}".format(tally, n, t, m)


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
        configs.append(make_args(port, n, t, test='acs', output=DIR + str(port) + '.out'))
    for i in range(t):
        port = BAD_PORT + i
        configs.append(make_args(port, n, t, test='acs', failure=f, output=DIR + str(port) + '.out'))

    ps = run_subprocesses(NODE_CMD_PREFIX, configs)

    print "Test: ACS polling"
    poll_check_f(120, 5, ps, check_acs_files, n, t)
    print "Test: ACS test passed"


@pytest.mark.parametrize("n,t,f", [
    (4, 1, 'omission'),
    (7, 2, 'omission'),
    (19, 6, 'omission'),
])
def test_bracha(n, t, f, folder, discover):
    configs = [make_args(GOOD_PORT, n, t, test='bracha', output=DIR + str(GOOD_PORT) + '.out')]
    for i in range(n - t - 1):
        port = GOOD_PORT + 1 + i
        configs.append(make_args(port, n, t, output=DIR + str(port) + '.out'))
    for i in range(t):
        port = BAD_PORT + i
        configs.append(make_args(port, n, t, failure=f, output=DIR + str(port) + '.out'))

    ps = run_subprocesses(NODE_CMD_PREFIX, configs)

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
        configs.append(make_args(port, n, t, test='mo14', value=v, output=DIR + str(port) + '.out'))
    for i in range(t):
        port = BAD_PORT + i
        randv = random.randint(0, 1)
        configs.append(make_args(port, n, t, test='mo14', value=randv, failure=f, output=DIR + str(port) + '.out'))

    ps = run_subprocesses(NODE_CMD_PREFIX, configs)

    print "Test: Mo14 polling"
    poll_check_f(20, 5, ps, check_mo14_files, n, t, v)
    print "Test: Mo14 test passed"


if __name__ == '__main__':
    check_acs_files(19, 6)
