import Queue
import argparse
import sys
import logging
from base64 import b64encode, b64decode
from typing import Dict, Tuple

from twisted.python import log
from twisted.internet import reactor
from twisted.internet.endpoints import TCP4ClientEndpoint, connectProtocol
from twisted.internet.error import CannotListenError
from twisted.internet.protocol import Factory
from twisted.internet.task import LoopingCall

from src.utils.jsonreceiver import JsonReceiver
from src.utils.messages import DummyMsg, PingMsg, PongMsg, BrachaMsg, Mo14Msg, ACSMsg, ChainMsg, SigMsg, CpMsg, ConsMsg
from src.utils.utils import Replay, Handled, set_logging
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
            logging.debug("NODE: processing item in queue")
            ctr += 1
            m = self.q.get()
            self.obj_received(m)

    def connection_lost(self, reason):
        logging.info("deleting peer {}, reason {}".format(b64encode(self.remote_vk), reason))
        try:
            del self.peers[self.remote_vk]
        except KeyError:
            logging.warning("peer {} already deleted".format(b64encode(self.remote_vk)))

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
            self.process_acs_res(res, obj)

        elif isinstance(obj, ChainMsg):
            self.factory.tc_runner.handle(obj.body, self.remote_vk)

        elif isinstance(obj, SigMsg):
            self.factory.tc_runner.handle_sig(obj, self.remote_vk)

        elif isinstance(obj, CpMsg):
            self.factory.tc_runner.handle_cp(obj, self.remote_vk)

        elif isinstance(obj, ConsMsg):
            self.factory.tc_runner.handle_cons(obj, self.remote_vk)

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
            logging.info("got dummy message from {}".format(b64encode(self.remote_vk)))

        else:
            raise AssertionError("invalid message type {}".format(obj))

    def process_acs_res(self, o, m):
        """

        :param o: the object we're processing
        :param m: the original message
        :return:
        """
        assert o is not None

        if isinstance(o, Replay):
            logging.debug("putting {} into msg queue".format(m))
            self.q.put(m)
        elif isinstance(o, Handled):
            if self.factory.config.test == 'acs':
                logging.debug("testing ACS, not handling the result")
                return
            if o.m is not None:
                logging.debug("attempting to handle ACS result")
                self.factory.tc_runner.handle_cons_from_acs(o.m)
        else:
            raise AssertionError("instance is not Replay or Handled")

    def send_ping(self):
        self.send_obj(PingMsg(self.vk, self.config.port))
        logging.debug("sent ping")
        self.state = 'CLIENT'

    def handle_ping(self, msg):
        # type: (PingMsg) -> None
        logging.debug("got ping, {}".format(msg))
        assert (self.state == 'SERVER')
        if msg.vk in self.peers.keys():
            logging.debug("ping found myself in peers.keys")
            # self.transport.loseConnection()
        self.peers[msg.vk] = (self.transport.getPeer().host, msg.port, self)
        self.remote_vk = msg.vk
        self.send_obj(PongMsg(self.vk, self.config.port))
        logging.debug("sent pong")

    def handle_pong(self, msg):
        # type: (PongMsg) -> None
        logging.debug("got pong, {}".format(msg))
        assert (self.state == 'CLIENT')
        if msg.vk in self.peers.keys():
            logging.debug("pong: found myself in peers.keys")
            # self.transport.loseConnection()
        self.peers[msg.vk] = (self.transport.getPeer().host, msg.port, self)
        self.remote_vk = msg.vk
        logging.debug("done pong")


class MyFactory(Factory):
    """
    The Twisted Factory with a broadcast functionality, should be singleton
    """
    def __init__(self, config):
        # type: (Config) -> None
        self.peers = {}  # type: Dict[str, Tuple[str, int, MyProto]]
        self.promoters = []
        self.config = config
        self.bracha = Bracha(self)  # just for testing
        self.mo14 = Mo14(self)  # just for testing
        self.acs = ACS(self)
        self.tc_runner = TrustChainRunner(self, lambda m: ChainMsg(m))
        self.vk = self.tc_runner.tc.vk

    def buildProtocol(self, addr):
        return MyProto(self)

    def new_connection_if_not_exist(self, nodes):
        for _vk, addr in nodes.iteritems():
            vk = b64decode(_vk)
            if vk not in self.peers.keys() and vk != self.vk:
                host, port = addr.split(":")
                self.make_new_connection(host, int(port))
            else:
                logging.debug("client {},{} already exist".format(b64encode(vk), addr))

    def make_new_connection(self, host, port):
        logging.debug("making client connection {}:{}".format(host, port))
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

    def promoter_cast(self, msg):
        try:
            for promoter in self.promoters:
                self.send(promoter, msg)
        except KeyError as e:
            logging.exception(e)
            raise

    def non_promoter_cast(self, msg):
        for node in set(self.peers.keys()) - set(self.promoters):
            self.send(node, msg)

    def send(self, node, msg):
        proto = self.peers[node][2]
        proto.send_obj(msg)

    def overwrite_promoters(self):
        """
        sets all peers to promoters, only use this method for testing
        :return:
        """
        logging.debug("overwriting promoters {}".format(len(self.peers)))
        self.promoters = self.peers.keys()

    def fill_promoters(self):
        """
        This should not happen during normal circumstances, only happens when testing when everybody is a promoter
        :return:
        """
        if len(self.promoters) < self.config.n:
            logging.debug("not enough promoters, picking one deterministically")
            candidates = sorted(list(set(self.peers.keys()) - set(self.promoters)))
            for i in range(self.config.n - len(self.promoters)):
                logging.debug("adding {} to promoter".format(b64encode(candidates[i])))
                self.promoters.append(candidates[i])


def got_protocol(p):
    # this needs to be lower than the callLater in `run`
    reactor.callLater(1, p.send_ping)


class Config:
    """
    All the static settings, used in Factory
    Should be singleton
    """
    def __init__(self, port, n, t, output, loglevel=logging.INFO, test=None, value=0, failure=None, tx=0):
        """
        This only stores the config necessary at runtime, so not necessarily all the information from argparse
        :param port:
        :param n:
        :param t:
        :param output:
        :param loglevel:
        :param test:
        :param value:
        :param failure:
        :param tx:
        """
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

        set_logging(loglevel, output)


def run(config, bcast):
    f = MyFactory(config)

    try:
        reactor.listenTCP(config.port, f)
    except CannotListenError:
        logging.warning("cannot listen on {}".format(config.port))
        sys.exit(1)

    # connect to discovery server
    point = TCP4ClientEndpoint(reactor, "localhost", 8123)
    d = connectProtocol(point, Discovery({}, f))
    d.addCallback(got_discovery, b64encode(f.vk), config.port).addErrback(log.err)

    # connect to myself
    point = TCP4ClientEndpoint(reactor, "localhost", config.port)
    d = connectProtocol(point, MyProto(f))
    d.addCallback(got_protocol).addErrback(log.err)

    if bcast:
        reactor.callLater(5, f.overwrite_promoters)

    # optionally run tests, args.test == None implies reactive node
    # we use call later to wait until the nodes are registered
    if config.test == 'dummy':
        reactor.callLater(5, f.bcast, DummyMsg('z'))
    elif config.test == 'bracha':
        reactor.callLater(6, f.bracha.bcast_init)
    elif config.test == 'mo14':
        reactor.callLater(6, f.mo14.start, config.value)
    elif config.test == 'acs':
        reactor.callLater(6, f.acs.start, config.port, 1)  # use port number (unique on local network) as test message
    elif config.test == 'tc':
        if config.tx > 0:
            reactor.callLater(5, f.tc_runner.make_random_tx)
    elif config.test == 'bootstrap':
        # TODO for now everybody is a promoter
        reactor.callLater(5, f.tc_runner.bootstrap_promoters)

    reactor.run()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'port',
        type=int, help='the listener port'
    )
    parser.add_argument(
        'n',
        type=int, help='the total number of promoters'
    )
    parser.add_argument(
        't',
        type=int, help='the total number of malicious nodes'
    )
    parser.add_argument(
        '-d', '--debug',
        help="log at debug level",
        action="store_const", dest="loglevel", const=logging.DEBUG,
        default=logging.WARNING
    )
    parser.add_argument(
        '-v', '--verbose',
        help="log at info level",
        action="store_const", dest="loglevel", const=logging.INFO
    )
    parser.add_argument(
        "-o", "--output",
        type=argparse.FileType('w'),
        metavar='NAME',
        help="location for the output file"
    )
    parser.add_argument(
        '--test',
        choices=['dummy', 'bracha', 'mo14', 'acs', 'tc', 'bootstrap'],
        help='[testing] choose an algorithm to initialise'
    )
    parser.add_argument(
        '--value',
        choices=[0, 1],
        default=0,
        type=int,
        help='[testing] the initial input for BA'
    )
    parser.add_argument(
        '--failure',
        choices=['byzantine', 'omission'],
        help='[testing] the mode of failure'
    )
    parser.add_argument(
        '--tx',
        type=int,
        metavar='RATE',
        default=0,
        help='[testing] initiate transaction at RATE/sec'
    )
    parser.add_argument(
        '--broadcast',
        help='[testing] overwrite promoters to be all peers',
        action='store_true'
    )
    args = parser.parse_args()

    run(Config(args.port, args.n, args.t, args.output, args.loglevel, args.test, args.value, args.failure, args.tx),
        args.broadcast)
