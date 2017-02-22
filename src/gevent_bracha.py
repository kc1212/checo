import gevent
import random
from gevent.queue import Queue
from gevent import Greenlet

n = 4
t = 1
queues = [Queue() for _ in range(n)]
tasks = []


def broadcast(i, msg):
    print("broadcasting..", i, msg)
    for q in queues:
        q.put((i, msg))


def bracha(i, n, t, q):
    echo_count = 0
    init_count = 0
    ready_count = 0
    step = 1

    # TODO check round and message body

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
            raise

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


if __name__ == "__main__":
    for (i, q) in zip(range(n), queues):
        if i == 0:
            tasks.append(gevent.spawn(init_bracha, i, "xaxa"))
        tasks.append(gevent.spawn(bracha, i, n, t, q))

    # tasks.append(gevent.sleep(5))
    gevent.joinall(tasks)
