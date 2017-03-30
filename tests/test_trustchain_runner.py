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


@pytest.fixture
def run_everything(n, t, m, failure):
    configs = []
    for i in range(m - t):
        port = GOOD_PORT + i
        configs.append(make_args(port, n, t, test='bootstrap', output=DIR + str(port) + '.out', broadcast=False))
    for i in range(t):
        port = BAD_PORT + i
        configs.append(make_args(port, n, t, test='bootstrap', output=DIR + str(port) + '.out', broadcast=False,
                                 failure=failure))

    ps = run_subprocesses(NODE_CMD_PREFIX, configs)
    print "Test: nodes starting"

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
def test_everything(n, t, m, failure, folder, discover):
    run_everything(n, t, m, failure)
