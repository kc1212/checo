from twisted.internet.endpoints import TCP4ClientEndpoint, connectProtocol
from twisted.internet.protocol import Factory
from twisted.internet import reactor
from twisted.internet.error import CannotListenError
import sys
import uuid
import argparse

from bracha import Bracha
from mo14 import Mo14
from discovery import Discovery, got_discovery
from messages import Payload, PayloadType
from jsonreceiver import JsonReceiver


class MyProto(JsonReceiver):
    def __init__(self, factory):
        self.factory = factory
        self.config = factory.config
        self.peers = factory.peers  # NOTE does not include myself
        self.remote_id = None
        self.state = 'SERVER'

    def connection_lost(self, reason):
        print "deleting peer ", self.remote_id
        try:
            del self.peers[self.remote_id]
        except KeyError:
            print "peer already deleted", self.remote_id, "my id is", self.config.id

    def json_received(self, obj):
        """
        struct Payload {
            payload_type: u32,
            payloada: Msg, // any type, passed directly to sub system
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

        elif ty == PayloadType.bracha.value:
            self.factory.bracha.handle(payload.payload)

        elif ty == PayloadType.mo14.value:
            self.factory.mo14.handle(payload.payload, self.remote_id)

        elif ty == PayloadType.dummy.value:
            print "got dummy message from", self.remote_id
        else:
            pass

        # self.print_info()

    def send_ping(self):
        self.send_json(Payload.make_ping((config.id.urn, config.port)).to_dict())
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
        self.send_json(Payload.make_pong((config.id.urn, config.port)).to_dict())
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


# singleton
class MyFactory(Factory):
    def __init__(self, config):
        self.peers = {}  # key: uuid, value: (host: str, port: int, self: MyProto)
        self.config = config
        self.bracha = Bracha(self)
        self.mo14 = Mo14(self)

    def buildProtocol(self, addr):
        return MyProto(self)

    def new_connection_if_not_exist(self, nodes):
        for id, addr in nodes.iteritems():
            id = uuid.UUID(id)
            if id not in self.peers.keys() and id != config.id:
                host, port = addr.split(":")
                self.make_new_connection(host, int(port))
            else:
                print "client already exist", id, addr

    def make_new_connection(self, host, port):
        print "making client connection", host, port
        point = TCP4ClientEndpoint(reactor, host, port)
        proto = MyProto(self)
        d = connectProtocol(point, proto)
        d.addCallback(got_protocol).addErrback(error_back)

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


# singleton
class Config:
    def __init__(self, n, t, port):
        self.n = n
        self.t = t
        self.id = uuid.uuid4()
        self.port = port


def error_back(failure):
    sys.stderr.write(str(failure))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('port', type=int, help='the listener port')
    parser.add_argument('n', type=int, help='the total number of promoters')
    parser.add_argument('t', type=int, help='the total number of malicious nodes')
    parser.add_argument('--test', choices=['dummy', 'bracha', 'bv_bcast'],
                        help='this is for testing, choose which algorithm to initialise, '
                             'empty selection runs a purely reactive node')
    args = parser.parse_args()

    config = Config(args.n, args.t, args.port)
    f = MyFactory(config)

    try:
        reactor.listenTCP(config.port, f)
    except CannotListenError:
        print("cannot listen on ", config.port)
        sys.exit(1)

    # connect to discovery server
    point = TCP4ClientEndpoint(reactor, "localhost", 8123)
    d = connectProtocol(point, Discovery({}, f))
    d.addCallback(got_discovery, config.id.urn, config.port).addErrback(error_back)

    # connect to myself
    point = TCP4ClientEndpoint(reactor, "localhost", config.port)
    d = connectProtocol(point, MyProto(f))
    d.addCallback(got_protocol).addErrback(error_back)

    # optionally run tests, args.test == None implies reactive node
    if args.test == 'dummy':
        reactor.callLater(5, f.bcast, Payload.make_dummy("z").to_dict())
    elif args.test == 'bracha':
        reactor.callLater(5, f.bracha.bcast_init)
    elif args.test == 'bv_bcast':
        reactor.callLater(5, f.mo14.start, 1)
        pass

    reactor.run()
