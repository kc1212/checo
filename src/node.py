import Queue
import argparse
import sys
import uuid

from consensus.acs import ACS
from consensus.mo14 import Mo14
from twisted.internet import reactor
from twisted.internet.endpoints import TCP4ClientEndpoint, connectProtocol
from twisted.internet.error import CannotListenError
from twisted.internet.protocol import Factory
from twisted.internet.task import LoopingCall
from utils.jsonreceiver import JsonReceiver
from utils.messages import Payload, PayloadType

from bracha import Bracha
from src.discovery import Discovery, got_discovery
from utils import Replay, Handled


class MyProto(JsonReceiver):
    """
    Main protocol that handles the Byzantine consensus, one instance is created for each connection
    """
    def __init__(self, factory):
        self.factory = factory
        self.config = factory.config
        self.q = factory.q
        self.peers = factory.peers  # NOTE does not include myself
        self.remote_id = None
        self.state = 'SERVER'

        # start looping call on the queue
        self.lc = LoopingCall(self.process_queue)
        self.lc.start(5)

    def process_queue(self):
        qsize = self.q.qsize()
        print "processing {} items in queue".format(qsize)

        # we use counter to stop this routine from running forever,
        # because self.json_received can put item back into the queue
        ctr = 0
        while not self.q.empty() and ctr < qsize:
            ctr += 1
            m = self.factory.q.get()
            self.json_received(m)

    def connection_lost(self, reason):
        print "deleting peer ", self.remote_id
        try:
            del self.peers[self.remote_id]
        except KeyError:
            print "peer already deleted", self.remote_id, "my id is", self.config.id

    def json_received(self, obj):
        """
        first we handle the items in the queue
        then we handle the received message
        struct Payload {
            payload_type: u32,
            payload: Msg, // any type, passed directly to sub system
        }
        :param obj:
        :return:
        """
        payload = Payload.from_dict(obj)
        ty = payload.payload_type

        if ty == PayloadType.ping.value:
            self.handle_ping(payload.payload)

        elif ty == PayloadType.pong.value:
            self.handle_pong(payload.payload)

        elif ty == PayloadType.acs.value:
            if self.factory.config.silent:
                return
            res = self.factory.acs.handle(payload.payload, self.remote_id)
            self.check_and_add_to_queue(res, obj)

        elif ty == PayloadType.chain.value:
            pass

        # messages below are for testing, bracha/mo14 is handled by acs
        elif ty == PayloadType.bracha.value:
            if self.factory.config.silent:
                return
            self.factory.bracha.handle(payload.payload)

        elif ty == PayloadType.mo14.value:
            if self.factory.config.silent:
                return
            self.factory.mo14.handle(payload.payload, self.remote_id)

        elif ty == PayloadType.dummy.value:
            print "got dummy message from", self.remote_id
        else:
            print "invalid message type"
            raise AssertionError

            # self.print_info()

    def check_and_add_to_queue(self, o, m):
        assert o is not None
        assert isinstance(o, Handled) or isinstance(o, Replay)

        if isinstance(o, Replay):
            print "putting {} into msg queue".format(m)
            self.q.put(m)

    def send_ping(self):
        self.send_json(Payload.make_ping((self.config.id.urn, self.config.port)).to_dict())
        print "sent ping"
        self.state = 'CLIENT'

    def handle_ping(self, msg):
        print "got ping", msg
        assert (self.state == 'SERVER')
        _id, _port = msg
        if uuid.UUID(_id) in self.peers.keys():
            print "ping found myself in peers.keys"
            # self.transport.loseConnection()
        self.peers[uuid.UUID(_id)] = (self.transport.getPeer().host, _port, self)
        self.send_json(Payload.make_pong((self.config.id.urn, self.config.port)).to_dict())
        self.remote_id = uuid.UUID(_id)
        print "sent pong"

    def handle_pong(self, msg):
        print "got pong", msg
        assert (self.state == 'CLIENT')
        _id, _port = msg
        if uuid.UUID(_id) in self.peers.keys():
            print "pong: found myself in peers.keys"
            # self.transport.loseConnection()
        self.peers[uuid.UUID(_id)] = (self.transport.getPeer().host, _port, self)
        self.remote_id = uuid.UUID(_id)
        print "done pong"

    def print_info(self):
        print "info: me: {}, remote: {}, peers: {}".format(self.config.id, self.remote_id, self.peers.keys())


class MyFactory(Factory):
    """
    The Twisted Factory with a broadcast functionality, should be singleton
    """
    def __init__(self, config):
        self.peers = {}  # key: uuid, value: (host: str, port: int, self: MyProto)
        self.config = config
        self.bracha = Bracha(self)  # just for testing
        self.mo14 = Mo14(self)  # just for testing
        self.acs = ACS(self)
        self.q = Queue.Queue()  # used for replaying un-handled messages

    def buildProtocol(self, addr):
        return MyProto(self)

    def new_connection_if_not_exist(self, nodes):
        for id, addr in nodes.iteritems():
            id = uuid.UUID(id)
            if id not in self.peers.keys() and id != self.config.id:
                host, port = addr.split(":")
                self.make_new_connection(host, int(port))
            else:
                print "client already exist", id, addr

    def make_new_connection(self, host, port):
        print "making client connection", host, port
        point = TCP4ClientEndpoint(reactor, host, port)
        proto = MyProto(self)
        d = connectProtocol(point, proto)
        d.addCallback(got_protocol)  # .addErrback(error_back)

    def bcast(self, msg):
        """
        Broadcast a message to all nodes in self.peers, the list should include myself
        :param msg: dictionary that can be converted into json via send_json
        :return:
        """
        for k, v in self.peers.iteritems():
            proto = v[2]
            proto.send_json(msg)


def got_protocol(p):
    reactor.callLater(1, p.send_ping)


class Config:
    """
    All the static settings, used in Factory
    Should be singleton
    """
    def __init__(self, port, n, t, test=None, value=None, byzantine=None, silent=None):
        self.port = port
        self.n = n
        self.t = t
        self.id = uuid.uuid4()
        self.test = test

        self.value = value
        if self.value is not None:
            self.value = int(self.value)
            assert self.value in (0, 1)

        self.byzantine = False
        if byzantine:
            self.byzantine = True

        self.silent = False
        if silent:
            self.silent = True

    def make_args(self):
        res = [str(self.port), str(self.n), str(self.t)]
        if self.test is not None:
            res.append('--test')
            res.append(self.test)
        if self.value is not None:
            res.append('--value')
            res.append(str(self.value))
        if self.byzantine:
            res.append('--byzantine')
        if self.silent:
            res.append('--silent')
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
    d.addCallback(got_discovery, config.id.urn, config.port)  # .addErrback(error_back)

    # connect to myself
    point = TCP4ClientEndpoint(reactor, "localhost", config.port)
    d = connectProtocol(point, MyProto(f))
    d.addCallback(got_protocol)  # .addErrback(error_back)

    # optionally run tests, args.test == None implies reactive node
    if config.test == 'dummy':
        reactor.callLater(5, f.bcast, Payload.make_dummy("z").to_dict())
    elif config.test == 'bracha':
        reactor.callLater(5, f.bracha.bcast_init)
    elif config.test == 'mo14':
        reactor.callLater(1, f.mo14.delayed_start, config.value)
    elif config.test == 'acs':
        reactor.callLater(5, f.acs.start, config.port)  # use port number (unique on local network) as test message

    reactor.run()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('port', type=int, help='the listener port')
    parser.add_argument('n', type=int, help='the total number of promoters')
    parser.add_argument('t', type=int, help='the total number of malicious nodes')
    parser.add_argument('--test', choices=['dummy', 'bracha', 'mo14', 'acs'],
                        help='[for testing] choose an algorithm to initialise')
    parser.add_argument('--value', choices=['0', '1'], default='1',
                        help='[for testing] the initial input for BA')
    parser.add_argument('--byzantine', action="store_true",
                        help='[for testing] whether the node is Byzantine')
    parser.add_argument('--silent', action="store_true",
                        help='[for testing] whether the node is silent (omission)')
    args = parser.parse_args()

    run(Config(args.port, args.n, args.t, args.test, args.value, args.byzantine, args.silent))
