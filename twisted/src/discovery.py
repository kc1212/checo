from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineOnlyReceiver
from twisted.internet import reactor
import json
import uuid

from utils import byteify


class Discovery(LineOnlyReceiver):
    def __init__(self, nodes, node_factory=None):
        self.nodes = nodes
        self.id = None
        self.addr = None
        self.state = 'SERVER'
        self.factory = node_factory

    def connectionLost(self, reason):
        if self.id in self.nodes:
            del self.nodes[self.id]
            print "deleted", self.id

    def lineReceived(self, line):
        print "received", line

        if self.state == 'SERVER':
            # line must in the format: [str, str], received sayHello
            (_id, _port) = byteify(json.loads(line))
            self.id = uuid.UUID(_id).urn
            self.addr = self.transport.getPeer().host + ":" + str(_port)

            # TODO check addr to be in the form host:port
            if self.id not in self.nodes:
                print "added node", self.id, self.addr
                self.nodes[self.id] = self.addr

            self.sendLine(json.dumps(self.nodes))

        elif self.state == 'CLIENT':
            nodes = byteify(json.loads(line))
            print "making new clients...", nodes
            self.factory.new_connection_if_not_exist(nodes)

    def sayHello(self, id, port):
        self.state = 'CLIENT'
        self.sendLine(json.dumps((id, port)))
        print "discovery sent", id, port


class DiscoveryFactory(Factory):
    def __init__(self):
        # TODO do delete
        self.nodes = {}  # key = uuid, val = addr

    def buildProtocol(self, addr):
        return Discovery(self.nodes)


def got_discovery(p, id, port):
    p.sayHello(id, port)


if __name__ == '__main__':
    reactor.listenTCP(8123, DiscoveryFactory())
    print "discovery server running..."
    reactor.run()
