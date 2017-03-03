from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineOnlyReceiver
from twisted.internet import reactor
import json
import uuid

from utils import byteify
from jsonreceiver import JsonReceiver
from messages import Payload, PayloadType


class Discovery(JsonReceiver):
    """
    this is both a discovery server and a coin server
    """
    def __init__(self, nodes, node_factory=None):
        self.nodes = nodes
        self.id = None
        self.addr = None
        self.state = 'SERVER'
        self.factory = node_factory

    def connection_lost(self, reason):
        if self.id in self.nodes:
            del self.nodes[self.id]
            print "deleted", self.id

    def json_received(self, msg):
        print "received", msg

        payload = Payload.from_dict(msg)
        ty = payload.payload_type

        if self.state == 'SERVER':
            if ty == PayloadType.discover.value:
                (_id, _port) = payload.payload
                self.id = uuid.UUID(_id).urn
                self.addr = self.transport.getPeer().host + ":" + str(_port)

                # TODO check addr to be in the form host:port
                if self.id not in self.nodes:
                    print "added node", self.id, self.addr
                    self.nodes[self.id] = self.addr

                self.send_json(Payload.make_discover_reply(self.nodes).to_dict())

            elif ty == PayloadType.coin.value:
                pass

            else:
                print "invalid payload type on SERVER", ty
                raise AssertionError

        elif self.state == 'CLIENT':
            if ty == PayloadType.discover_reply.value:
                nodes = payload.payload
                print "making new clients...", nodes
                self.factory.new_connection_if_not_exist(nodes)

            elif ty == PayloadType.coin_reply.value:
                pass

            else:
                print "invalid payload type on CLIENT", ty
                raise AssertionError

    def say_hello(self, uuid, port):
        self.state = 'CLIENT'
        self.send_json(Payload.make_discover((uuid, port)).to_dict())
        print "discovery sent", uuid, port


class DiscoveryFactory(Factory):
    def __init__(self):
        self.nodes = {}  # key = uuid, val = addr

    def buildProtocol(self, addr):
        return Discovery(self.nodes)


def got_discovery(p, id, port):
    p.say_hello(id, port)


if __name__ == '__main__':
    reactor.listenTCP(8123, DiscoveryFactory())
    print "discovery server running..."
    reactor.run()
