import gevent
import random
from collections import defaultdict
from gevent.queue import Queue, Empty
# from gevent import Greenlet


n = 4
t = 1
queues = [Queue() for _ in range(n)]
coin_queues = [Queue() for _ in range(n)]
coin_req_queue = Queue()


def broadcast(msg):
    # print("broadcasting..", msg)
    for q in queues:
        q.put(msg)


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
    est_queue = [Queue(1)]
    aux_queue = [Queue(1)]

    # we need to forward the messages to the appropriate queues
    def forward_msg():
        while True:
            (tag, msg) = q.get()
            r, m = msg
            if tag == 'EST':
                est_queue[r].put(m)
            elif tag == 'COIN':
                # TODO coins use its separate queue, we need to share the queue
                pass
            elif tag == 'AUX':
                aux_queue[r].put(m)
            else:
                raise AssertionError
    gevent.spawn(forward_msg)

    def wait_for_aux(r, bin_values_q):
        aux_cnt = [0, 0]
        valuesi = []
        while True:
            v = aux_queue[r].get()
            aux_cnt[v] += 1
            if aux_cnt[v] >= n - t:
                # TODO more than 1 value in bin_values_q?
                candidate = bin_values_q.get()
                if candidate == aux_cnt[v]:
                    valuesi.append(candidate)
                    return valuesi

    while True:
        ri += 1
        est_queue.append(Queue(1))
        aux_queue.append(Queue(1))

        def bc_est(m):
            bc(('EST', (ri, m)))

        def bc_aux(m):
            bc(('AUX', (ri, m)))

        # exchange EXT
        out_queue = Queue()
        bv_broadcast(i, n, t, bc_est, est_queue[ri], out_queue, est)

        # exchange AUX
        w = random.choice(out_queue.peak())  # don't remove the item, we use it later
        bc_aux(w)
        valuesi = wait_for_aux(ri, out_queue)

        # local computation
        s = get_common_coin(i, ri)
        if len(valuesi) == 1:
            if valuesi[0] == s:
                print("node", i, "decided...", s)
                return s

            est = valuesi[0]
        else:
            est = s


def empty_queue(q):
    while True:
        q.get()


def bv_broadcast(i, n, t, bc, q, out, vi):
    table = [{}, {}]
    broadcasted = False
    values = []

    assert i in range(n)
    assert n > 3 * t

    def _bc(m):
        print("bv broadcasting..", i, m)
        bc((i, m))

    _bc(vi)

    while True:
        (j, v) = q.get()
        assert v in (0, 1)

        table[v][j] = True

        if len(table[v]) >= t + 1 and not broadcasted:
            print("node", i, "relaying", v)
            _bc(v)
            broadcasted = True

        if len(table[v]) >= 2 * t + 1:
            if v not in values:
                values.append(v)
                print("node", i, "deliver", values)
                out.put(v)
            if len(values) == 2:
                print("node", i, "deliver two things", values)
                # empty_queue(out)
                return


def bracha(i, n, t, q):
    echo_count = 0
    init_count = 0
    ready_count = 0
    step = 1
    # TODO check round and message body

    # TODO broadcast in args
    def _bc(ty, msg):
        return broadcast((i, (ty, msg)))

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
    broadcast((i, ('init', msg)))


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
    out_queues = [Queue(2) for _ in range(n)]
    for (i, q) in zip(range(n), queues):
        print("test_bv_broadcast", i, vi)
        if i % 2 == 0:
            pass
            tasks.append(gevent.spawn(bv_broadcast, i, n, t, bc, q, out_queues[i], random.randint(0, 1)))
        else:
            tasks.append(gevent.spawn(bv_broadcast, i, n, t, bc, q, out_queues[i], vi))

    for (i, q) in zip(range(n), out_queues):
        print("trying to extract results")
        vs = []
        try:
            vs.append(q.get())
            print("GOT... node", i, "returned", vs)
        except Empty:
            print("DONE... node", i, "returned", vs)
            continue

    try:
        gevent.joinall(tasks)
    except gevent.hub.LoopExit:
        print("End")

def test_common_coin():
    run_common_coin()
    for r in range(10):
        for i in range(n):
            print("round", r, "node", i, "coin", get_common_coin(i, r))

def test_consensus():
    vi = random.randint(0, 1)
    tasks = []
    for (i, q) in zip(range(n), queues):
        print("test_consensus", i, vi)
        tasks.append(gevent.spawn(binary_consensus, i, n, t, broadcast, q, vi))

    try:
        gevent.joinall(tasks)
    except gevent.hub.LoopExit:
        print("End")
if __name__ == "__main__":
    # test_bv_broadcast()
    # test_common_coin()
    test_consensus()

