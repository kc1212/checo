
from multiprocessing import Process
import node


def run(config):
    print 'Test: running config', config
    node.run(config)


def test_simple_acs():
    configs = [
        node.Config(12345, 4, 1, test='acs'),
        node.Config(12346, 4, 1, test='acs'),
        node.Config(12347, 4, 1, test='acs'),
        node.Config(12348, 4, 1, silent=True)
    ]

    ps = []
    for cfg in configs:
        p = Process(target=run, args=(cfg,))
        p.start()
        ps.append(p)

    for p in ps:
        p.join()

if __name__ == '__main__':
    test_simple_acs()
