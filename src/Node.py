from twisted.internet.endpoints import TCP4ClientEndpoint, connectProtocol
from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineOnlyReceiver
from twisted.internet import reactor
from twisted.internet.error import CannotListenError
import json
import sys
import uuid

from utils import byteify, PayloadType
from Bracha import Bracha
from Discovery import Discovery, got_discovery

def connect_client(peer, proto):
    host, port = peer.split(":")
    port = int(port)
    point = TCP4ClientEndpoint(reactor, host, int(port))
    d = connectProtocol(point, proto)
    # d.addCallback(got_protocol)\
    d.addErrback(error_back)


class JsonReceiver(LineOnlyReceiver):
    def lineReceived(self, line):
        obj = byteify(json.loads(line))
        self.jsonReceived(obj)

    def jsonReceived(self, obj):
        raise NotImplementedError

    def sendJSON(self, obj):
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

    def jsonReceived(self, obj):
        ty = obj["payload_type"]
        if ty == PayloadType.ping.value:
            self.handle_ping(obj["payload"])
        elif ty == PayloadType.pong.value:
            self.handle_pong(obj["payload"])
        elif ty == PayloadType.bracha.value:
            self.bracha.process(obj["payload"])
        elif ty == PayloadType.dummy.value:
            print "got dummy message from", self.remote_id
        else:
            pass

        self.printInfo()

    def send_ping(self):
        self.sendJSON({"payload_type": PayloadType.ping.value, "payload": (config.id.urn, config.port)})
        print "sent ping"
        self.state = 'CLIENT'

    def handle_ping(self, msg):
        print "got ping", msg
        assert(self.state == 'SERVER')
        _id, _port = msg
        if uuid.UUID(_id) in self.peers.keys():
            print "ping found myself in peers.keys"
            # self.transport.loseConnection()
        self.peers[uuid.UUID(_id)] = (self.transport.getPeer().host, _port, self)
        self.sendJSON({"payload_type": PayloadType.pong.value, "payload": (config.id.urn, config.port)})
        self.remote_id = uuid.UUID(_id)
        print "sent pong"

    def handle_pong(self, msg):
        print "got pong", msg
        assert(self.state == 'CLIENT')
        _id, _port = msg
        if uuid.UUID(_id) in self.peers.keys():
            print "pong: found myself in peers.keys"
            # self.transport.loseConnection()
        self.peers[uuid.UUID(_id)] = (self.transport.getPeer().host, _port, self)
        self.remote_id = uuid.UUID(_id)
        print "done pong"
        self.printInfo()

    # def handleHello(self, msg):
    #     id = uuid.UUID(msg)
    #     if id not in self.peers:
    #         self.peers[id] = (self, self.remote_peer)
    #
    # def sendHello(self):
    #     payload = {"payload_type": PayloadType.hello.value, "payload": self.config.id.urn}
    #     self.sendJSON(payload)
    #     print "send hello payload", payload
    #
    # def sendPeers(self):
    #     payload = self.makePeersPayload()
    #     self.sendJSON(payload)
    #     print "send peers payload", payload
    #
    # def handlePeers(self, results):
    #     for res in results:
    #         id = uuid.UUID(res[0])
    #         remote = res[1]
    #         if id == self.config.id:
    #             continue
    #         else:
    #             print "handle res", res
    #             connect_client(remote, MyProto(self.bracha))
    #     print "handled results", results
    #
    # def makePeersPayload(self):
    #     remotes = [v[1] for v in self.peers.values()]
    #     payload = zip(self.peers.keys(), remotes)
    #     return {"payload_type": PayloadType.peers.value, "payload": payload}

    def printInfo(self):
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
            proto.sendJSON(msg)


def got_protocol(p):
    reactor.callLater(1, p.send_ping)

# singleton
class Config:
    def __init__(self, n, t, port):
        self.n = n
        self.t = t
        self.id = uuid.uuid4()
        self.port = port


"""
struct Payload {
    payload_type: u32,
    payloada: Msg,
}

struct Msg {
    ty: u32,
    round: u32,
    body: String,
}
"""

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
        reactor.callLater(5, f.bcast, {"payload_type": PayloadType.dummy.value, "payload": "z"})
        reactor.callLater(6, f.bracha.bcast_init)

    reactor.run()
