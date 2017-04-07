from tools import *
import json


def check_multiple_rounds(n, t, max_r):
    for r in range(1, 1 + max_r):
        check_promoter_match(n, t, r)


def check_promoter_match(n, t, r):
    target = 'TC: updated new promoters in round {} to'.format(r)
    updated_promoters = search_for_string_in_dir(DIR, target, json.loads)
    assert len(updated_promoters) >= n - t

    for p in updated_promoters:
        p.sort()

    # change to tuple so that it's hashable
    vs = [tuple(p) for p in updated_promoters]
    v, tally = value_and_tally(vs)

    assert tally >= n - t


def run_consensus(n, t, m, failure):
    configs = []
    for i in range(m - t):
        port = GOOD_PORT + i
        configs.append(make_args(port, n, t, test='bootstrap', output=DIR + str(port) + '.out', broadcast=False))
    for i in range(t):
        port = BAD_PORT + i
        configs.append(make_args(port, n, t, test='bootstrap', output=DIR + str(port) + '.out', broadcast=False,
                                 failure=failure))

    ps = run_subprocesses(NODE_CMD_PREFIX, configs)
    print "Test: consensus nodes starting"

    # we use m instead of n because the consensus result should be propagated
    poll_check_f(8 * m, 5, ps, check_multiple_rounds, m, t, 3)


@pytest.mark.parametrize("n,t,m,failure", [
    (4, 1, 4, 'omission'),
    (4, 1, 8, 'omission'),
    (8, 2, 8, 'omission'),
    (8, 2, 16, 'omission'),
    # (19, 6, 19, 'omission'),
    # (19, 6, 30, 'omission'),
])
def test_consensus(n, t, m, failure, folder, discover):
    run_consensus(n, t, m, failure)


def check_tx(expected):
    target = 'TC: added tx'
    txs = search_for_all_string_in_dir(DIR, target, json.loads)

    # *2 because every tx creates 2 tx blocks
    print len(txs)
    assert len(txs) >= expected


@pytest.mark.parametrize("n,t,rate,timeout", [
    (4, 1, 1.0, 10),
    (4, 1, 5.0, 10),
    (8, 2, 2.0, 10),
    (8, 2, 10.0, 20),
])
def test_tx_periodically(n, t, rate, timeout, folder, discover):
    configs = []
    for i in range(n):
        port = GOOD_PORT + i
        configs.append(make_args(port, n, t, test='tc', tx_rate=rate / n, output=DIR + str(port) + '.out'))

    ps = run_subprocesses(NODE_CMD_PREFIX, configs)
    print "Test: tx nodes starting"

    # give it some time to setup
    time.sleep(timeout + 10)

    for p in ps:
        p.terminate()
    check_tx(rate * timeout * 2)
    print "Test: tx test passed"


@pytest.mark.parametrize("n, t, timeout, expected", [
    (4, 1, 10, 100),
])
def test_tx_continuously(n, t, timeout, expected, folder, discover):
    configs = []
    for i in range(n):
        port = GOOD_PORT + i
        configs.append(make_args(port, n, t, test='tc', tx_rate=-1.0, output=DIR + str(port) + '.out'))

    ps = run_subprocesses(NODE_CMD_PREFIX, configs)
    print "Test: tx nodes starting"

    time.sleep(timeout + 10)

    for p in ps:
        p.terminate()

    check_tx(100)
    print "Test: tx test passed"

