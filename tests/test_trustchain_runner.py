from tools import *


@pytest.fixture
def run_everything(n, t, f):
    configs = []
    for i in range(n):
        port = GOOD_PORT + i
        configs.append(make_args(port, n, t, test='bootstrap', output=DIR + str(port) + '.out', broadcast=False))
    # for i in range(t):
        # configs.append(make_args(port, n, t, test='bootstrap', failure=f, output=DIR + str(port) + '.out', broadcast=False))

    ps = run_subprocesses(NODE_CMD_PREFIX, configs)
    print "Test: nodes starting"

    # tear down
    yield None
    print "Test: tear down nodes"
    for p in ps:
        p.terminate()


@pytest.mark.parametrize("n,t,f,time_out", [
    (4, 1, 'omission', 20),
    # (7, 2, 'omission', 20),
    # (19, 6, 'omission', 20),
])
def test_everything(n, t, f, time_out, folder, discover, run_everything):
    time.sleep(time_out)
    pass
