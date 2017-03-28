from tools import *


@pytest.fixture
def run_everything(n, t, f, m):
    configs = []
    for i in range(m):
        port = GOOD_PORT + i
        configs.append(make_args(port, n, t, test='bootstrap', output=DIR + str(port) + '.out', broadcast=False))
    # for i in range(t):
        # configs.append(make_args(port, n, t, test='bootstrap', failure=f, output=DIR + str(port) + '.out', broadcast=False))

    ps = run_subprocesses(NODE_CMD_PREFIX, configs, 4./n)
    print "Test: nodes starting"

    # tear down
    yield None
    print "Test: tear down nodes"
    for p in ps:
        p.terminate()


@pytest.mark.parametrize("n,t,f,time_out,m", [
    (4, 1, 'omission', 35, 4),
    # (7, 2, 'omission', 20),
    # (19, 6, 'omission', 20),
])
def test_everything(n, t, f, time_out, folder, discover, run_everything):
    time.sleep(time_out)
    pass
