from twisted.internet.endpoints import TCP4ClientEndpoint, connectProtocol
from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineOnlyReceiver
from twisted.internet import reactor
from twisted.internet.error import CannotListenError
import json
import sys
import uuid

from utils import byteify
from Bracha import Bracha
from Discovery import Discovery, got_discovery
from messages import Payload, PayloadType


class JsonReceiver(LineOnlyReceiver):
    def lineReceived(self, line):
        obj = byteify(json.loads(line))
        self.json_received(obj)

    def json_received(self, obj):
        # we also expect a dict or list
        raise NotImplementedError

    def send_json(self, obj):
        # we expect dict or list
        self.sendLine(json.dumps(obj))


class MyProto(JsonReceiver):
    def __init__(self, factory):
        self.factory = factory
        self.config = factory.config
        self.bracha = factory.bracha
        self.peers = factory.peers  # NOTE does not include myself
        self.remote_id = None
        self.state = 'SERVER'

    def connectionLost(self, reason):
        print "deleting peer ", self.remote_id
        try:
            del (self.peers[self.remote_id])
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
            self.bracha.process(payload.payload)
        elif ty == PayloadType.dummy.value:
            print "got dummy message from", self.remote_id
        else:
            pass

        self.print_info()

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
        self.bracha = Bracha(self.peers, self.config)

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
    listen_port = int(sys.argv[1])
    config = Config(4, 1, listen_port)

    f = MyFactory(config)

    try:
        reactor.listenTCP(listen_port, f)
    except CannotListenError:
        print("cannot listen on ", listen_port)
        sys.exit(1)

    # connect to discovery server
    point = TCP4ClientEndpoint(reactor, "localhost", 8123)
    d = connectProtocol(point, Discovery({}, f))
    d.addCallback(got_discovery, config.id.urn, listen_port).addErrback(error_back)

    # connect to myself
    point = TCP4ClientEndpoint(reactor, "localhost", listen_port)
    d = connectProtocol(point, MyProto(f))
    d.addCallback(got_protocol).addErrback(error_back)

    # test dummy broadcast
    if listen_port == 12345:
        print "bcasting..."
        # reactor.callLater(5, f.bcast, Payload.make_dummy("z").to_dict())
        reactor.callLater(6, f.bracha.bcast_init)

    reactor.run()
