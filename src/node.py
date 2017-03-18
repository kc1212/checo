import Queue
import argparse
import sys
from base64 import b64encode, b64decode

from twisted.python import log
from twisted.internet import reactor
from twisted.internet.endpoints import TCP4ClientEndpoint, connectProtocol
from twisted.internet.error import CannotListenError
from twisted.internet.protocol import Factory
from twisted.internet.task import LoopingCall

from src.utils.jsonreceiver import JsonReceiver
from src.utils.messages import DummyMsg, PingMsg, PongMsg, BrachaMsg, Mo14Msg, ACSMsg, ChainMsg
from src.utils.utils import Replay, Handled
from src.consensus.bracha import Bracha
from src.consensus.acs import ACS
from src.consensus.mo14 import Mo14
from src.trustchain.trustchain_runner import TrustChainRunner
from .discovery import Discovery, got_discovery


class MyProto(JsonReceiver):
    """
    Main protocol that handles the Byzantine consensus, one instance is created for each connection
    """
    def __init__(self, factory):
        self.factory = factory
        self.config = factory.config
        self.vk = factory.vk
        self.q = Queue.Queue()  # used for replaying un-handled messages
        self.peers = factory.peers
        self.remote_vk = None
        self.state = 'SERVER'

        # start looping call on the queue
        self.lc = LoopingCall(self.process_queue)
        self.lc.start(1).addErrback(log.err)

    def process_queue(self):
        # we use counter to stop this routine from running forever,
        # because self.json_received can put item back into the queue
        qsize = self.q.qsize()
        ctr = 0
        while not self.q.empty() and ctr < qsize:
            print "NODE: processing item in queue"
            ctr += 1
            m = self.q.get()
            self.obj_received(m)

    def connection_lost(self, reason):
        print "deleting peer ", b64encode(self.remote_vk)
        try:
            del self.peers[self.remote_vk]
        except KeyError:
            print "peer {} already deleted".format(b64encode(self.remote_vk))

    def obj_received(self, obj):
        """
        first we handle the items in the queue
        then we handle the received message
        :param obj:
        :return:
        """

        if isinstance(obj, PingMsg):
            self.handle_ping(obj)

        elif isinstance(obj, PongMsg):
            self.handle_pong(obj)

        elif isinstance(obj, ACSMsg):
            if self.factory.config.failure == 'omission':
                return
            res = self.factory.acs.handle(obj, self.remote_vk)
            self.check_and_add_to_queue(res, obj)

        elif isinstance(obj, ChainMsg):
            self.factory.tc_runner.handle(obj.body, self.remote_vk)

        # NOTE messages below are for testing, bracha/mo14 is normally handled by acs
        elif isinstance(obj, BrachaMsg):
            if self.factory.config.failure == 'omission':
                return
            self.factory.bracha.handle(obj)

        elif isinstance(obj, Mo14Msg):
            if self.factory.config.failure == 'omission':
                return
            self.factory.mo14.handle(obj, self.remote_vk)

        elif isinstance(obj, DummyMsg):
            print "got dummy message from", b64encode(self.remote_vk)

        else:
            raise AssertionError("invalid message type")

    def check_and_add_to_queue(self, o, m):
        assert o is not None
        assert isinstance(o, Handled) or isinstance(o, Replay)

        if isinstance(o, Replay):
            print "putting {} into msg queue".format(m)
            self.q.put(m)

    def send_ping(self):
        self.send_obj(PingMsg(self.vk, self.config.port))
        print "sent ping"
        self.state = 'CLIENT'

    def handle_ping(self, msg):
        # type: (PingMsg) -> None
        print "got ping", msg
        assert (self.state == 'SERVER')
        if msg.vk in self.peers.keys():
            print "ping found myself in peers.keys"
            # self.transport.loseConnection()
        self.peers[msg.vk] = (self.transport.getPeer().host, msg.port, self)
        self.remote_vk = msg.vk
        self.send_obj(PongMsg(self.vk, self.config.port))
        print "sent pong"

    def handle_pong(self, msg):
        # type: (PongMsg) -> None
        print "got pong", msg
        assert (self.state == 'CLIENT')
        if msg.vk in self.peers.keys():
            print "pong: found myself in peers.keys"
            # self.transport.loseConnection()
        self.peers[msg.vk] = (self.transport.getPeer().host, msg.port, self)
        self.remote_vk = msg.vk
        print "done pong"


class MyFactory(Factory):
    """
    The Twisted Factory with a broadcast functionality, should be singleton
    """
    def __init__(self, config):
        self.peers = {}  # key: vk, value: (host: str, port: int, self: MyProto)
        self.config = config
        self.bracha = Bracha(self)  # just for testing
        self.mo14 = Mo14(self)  # just for testing
        self.acs = ACS(self)
        self.tc_runner = TrustChainRunner(self, lambda m: ChainMsg(m))
        self.vk = self.tc_runner.chain.vk

    def buildProtocol(self, addr):
        return MyProto(self)

    def new_connection_if_not_exist(self, nodes):
        for _vk, addr in nodes.iteritems():
            vk = b64decode(_vk)
            if vk not in self.peers.keys() and vk != self.vk:
                host, port = addr.split(":")
                self.make_new_connection(host, int(port))
            else:
                print "client already exist", b64encode(vk), addr

    def make_new_connection(self, host, port):
        print "making client connection", host, port
        point = TCP4ClientEndpoint(reactor, host, port)
        proto = MyProto(self)
        d = connectProtocol(point, proto)
        d.addCallback(got_protocol).addErrback(log.err)

    def bcast(self, msg):
        """
        Broadcast a message to all nodes in self.peers, the list should include myself
        :param msg: dictionary that can be converted into json via send_json
        :return:
        """
        for k, v in self.peers.iteritems():
            proto = v[2]
            proto.send_obj(msg)

    def send(self, node, msg):
        proto = self.peers[node][2]
        proto.send_obj(msg)


def got_protocol(p):
    # this needs to be lower than the callLater in `run`
    reactor.callLater(1, p.send_ping)


class Config:
    """
    All the static settings, used in Factory
    Should be singleton
    """
    def __init__(self, port, n, t, test=None, value=0, failure=None, tx=0):
        self.port = port
        self.n = n
        self.t = t
        self.test = test

        assert value in (0, 1)
        self.value = value

        # TODO use None or 'none' as default?
        assert failure == 'byzantine' or failure == 'omission' or failure is None
        self.failure = failure

        assert isinstance(tx, int)
        assert tx >= 0
        self.tx = tx

    def make_args(self):
        res = [str(self.port), str(self.n), str(self.t)]

        if self.test is not None:
            res.append('--test')
            res.append(self.test)

        if self.value is not None:
            res.append('--value')
            res.append(str(self.value))

        if self.failure is not None:
            res.append('--failure')
            res.append(self.failure)

        res.append('--tx')
        res.append(str(self.tx))

        return res


def run(config):
    f = MyFactory(config)

    try:
        reactor.listenTCP(config.port, f)
    except CannotListenError:
        print("cannot listen on ", config.port)
        sys.exit(1)

    # connect to discovery server
    point = TCP4ClientEndpoint(reactor, "localhost", 8123)
    d = connectProtocol(point, Discovery({}, f))
    d.addCallback(got_discovery, b64encode(f.vk), config.port).addErrback(log.err)

    # connect to myself
    point = TCP4ClientEndpoint(reactor, "localhost", config.port)
    d = connectProtocol(point, MyProto(f))
    d.addCallback(got_protocol).addErrback(log.err)

    # optionally run tests, args.test == None implies reactive node
    # we use call later to wait until the nodes are registered
    if config.test == 'dummy':
        reactor.callLater(5, f.bcast, DummyMsg('z'))
    elif config.test == 'bracha':
        reactor.callLater(5, f.bracha.bcast_init)
    elif config.test == 'mo14':
        reactor.callLater(5, f.mo14.start, config.value)
    elif config.test == 'acs':
        reactor.callLater(5, f.acs.start, config.port)  # use port number (unique on local network) as test message
    elif config.test == 'tc':
        if config.tx > 0:
            reactor.callLater(5, f.tc_runner.make_random_tx)

    reactor.run()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('port', type=int, help='the listener port')
    parser.add_argument('n', type=int, help='the total number of promoters')
    parser.add_argument('t', type=int, help='the total number of malicious nodes')
    parser.add_argument('--test', choices=['dummy', 'bracha', 'mo14', 'acs', 'tc'],
                        help='[for testing] choose an algorithm to initialise')
    parser.add_argument('--value', choices=[0, 1], default=0, type=int,
                        help='[testing] the initial input for BA')
    parser.add_argument('--failure', choices=['byzantine', 'omission'],
                        help='[testing] the mode of failure')
    parser.add_argument('--tx', type=int, metavar='RATE', default=0,
                        help='[testing] whether to initiate transaction RATE/sec')
    args = parser.parse_args()

    run(Config(args.port, args.n, args.t, args.test, args.value, args.failure, args.tx))
