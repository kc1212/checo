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


def print_profile_stats():
    import pstats
    p = pstats.Stats('profile.stats')
    p.sort_stats('cumulative').print_stats()


@pytest.mark.parametrize("n,t,m,failure,profile", [
    (4, 1, 4, 'omission', False),
    (4, 1, 8, 'omission', False),
    (8, 2, 8, 'omission', False),
    (8, 2, 16, 'omission', False),
    # (19, 6, 19, 'omission', True),  # uncomment this for profiling
    # (19, 6, 30, 'omission'),
])
def test_consensus(n, t, m, failure, profile, folder, discover):
    configs = []

    for i in range(m - t):
        port = GOOD_PORT + i
        if profile and i == 0:
            configs.append(make_args(port, n, t, profile=True, test='bootstrap', output=DIR + str(port) + '.out', broadcast=False))
        else:
            configs.append(make_args(port, n, t, test='bootstrap', output=DIR + str(port) + '.out', broadcast=False))

    for i in range(t):
        port = BAD_PORT + i
        configs.append(make_args(port, n, t, test='bootstrap', output=DIR + str(port) + '.out', broadcast=False,
                                 failure=failure))

    ps = run_subprocesses(NODE_CMD_PREFIX, configs)
    print "Test: consensus nodes starting"

    # we use m instead of n because the consensus result should be propagated
    poll_check_f(8 * m, 5, ps, check_multiple_rounds, m, t, 3)

    if profile:
        time.sleep(1)
        print_profile_stats()


def check_tx(expected):
    target = 'TC: current tx count'
    counts = search_for_last_string_in_dir(DIR, target, json.loads)

    # *2 because every tx creates 2 tx blocks
    print sum(counts)
    assert sum(counts) >= expected


@pytest.mark.parametrize("n,t,timeout,expected", [
    (4, 1, 20, 200),
    (8, 2, 20, 400),
])
def test_tx(n, t, timeout, expected, folder, discover):
    configs = []
    for i in range(n):
        port = GOOD_PORT + i
        configs.append(make_args(port, n, t, test='tc', tx_rate=5, output=DIR + str(port) + '.out'))

    ps = run_subprocesses(NODE_CMD_PREFIX, configs)
    print "Test: tx nodes starting"

    # give it some time to setup
    time.sleep(timeout + 6)

    for p in ps:
        p.terminate()

    check_tx(expected)
    print "Test: tx test passed"


