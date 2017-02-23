import gevent
import random
from gevent.queue import Queue
# from gevent import Greenlet


n = 4
t = 1
queues = [Queue() for _ in range(n)]
coin_queues = [Queue() for _ in range(n)]
coin_req_queue = Queue()


def broadcast(i, msg):
    print("broadcasting..", i, msg)
    for q in queues:
        q.put((i, msg))


def run_common_coin():
    def f():
        coins = []
        while True:
            (i, r) = coin_req_queue.get()
            assert r <= len(coins)  # round (begins at 0) cannot exceed the number of coins
            assert i in range(n)

            if len(coins) == r:
                v = random.randint(0, 1)
                coins.append(v)
                coin_queues[i].put((i, r, v))
            else:
                coin_queues[i].put((i, r, coins[r]))

    gevent.spawn(f)


def get_common_coin(i, r):
    coin_req_queue.put((i, r))
    (_i, _r, _v) = coin_queues[i].get()
    assert i == _i
    assert r == _r
    assert _v is not None
    return _v


def binary_consensus(i, n, t, bc, q, vi):
    est = vi
    ri = 0

    # we need to forward the messages to the appropriate queues
    def forward_msg():
        while True:
            (i, tag, m) = q.get()
            if tag == 'EST':
                pass
            elif tag == 'COIN':
                pass
            elif tag == 'AUX':
                pass
            else:
                raise AssertionError

    while True:
        ri += 1
        # TODO the q may contain results from another broadcast
        bv_broadcast(i, n, t, bc, q, est)

    raise RuntimeError


def bv_broadcast(i, n, t, bc, q, vi):
    table = [{}, {}]
    not_yet_bc = True
    values = []

    assert i in range(n)
    assert n > 3 * t

    bc(i, vi)

    while True:
        (j, v) = q.get()
        assert v in (0, 1)

        table[v][j] = True

        if len(table[v]) >= t + 1 and not_yet_bc:
            print("node", i, "relaying", v)
            bc(i, v)
            not_yet_bc = False

        if len(table[v]) >= 2 * t + 1:
            if v not in values:
                values.append(v)
                print("node", i, "deliver", values)
            if len(values) == 2:
                print("node", i, "deliver two things", values)
                return


def bracha(i, n, t, q):
    echo_count = 0
    init_count = 0
    ready_count = 0
    step = 1
    # TODO check round and message body

    # TODO broadcast in args
    def _bc(ty, msg):
        return broadcast(i, (ty, msg))

    def _enough_ready():
        if ready_count >= 2 * t + 1:
            return True
        return False

    def _ok_to_send():
        if echo_count >= (n + t) / 2 or ready_count >= (t + 1):
            return True
        return False

    while True:
        (j, (ty, msg)) = q.get()  # blocks
        if ty == 'init':
            init_count += 1
        elif ty == 'echo':
            echo_count += 1
        elif ty == 'ready':
            ready_count += 1
        else:
            raise AssertionError

        if step == 1:
            if init_count > 0 or _ok_to_send():
                _bc('echo', msg)
                step = 2
        elif step == 2:
            if _ok_to_send():
                _bc('ready', msg)
                step = 3
        elif step == 3:
            if _enough_ready():
                print("accept", i, msg)
                return


def init_bracha(i, msg):
    broadcast(i, ('init', msg))


def test_bracha():
    tasks = []
    for (i, q) in zip(range(n), queues):
        if i == 0:
            tasks.append(gevent.spawn(init_bracha, i, "xaxa"))
        tasks.append(gevent.spawn(bracha, i, n, t, q))
    gevent.joinall(tasks)


def test_bv_broadcast():
    tasks = []
    bc = broadcast
    vi = random.randint(0, 1)
    for (i, q) in zip(range(n), queues):
        print("test_bv_broadcast", i, vi)
        if i == 0:
            tasks.append(gevent.spawn(bv_broadcast, i, n, t, bc, q, random.randint(0, 1)))
        else:
            tasks.append(gevent.spawn(bv_broadcast, i, n, t, bc, q, vi))
    gevent.joinall(tasks)


def test_common_coin():
    run_common_coin()
    for r in range(10):
        for i in range(n):
            print("round", r, "node", i, "coin", get_common_coin(i, r))

if __name__ == "__main__":
    test_bv_broadcast()
    # test_common_coin()

