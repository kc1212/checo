from twisted.internet import reactor
from twisted.internet.protocol import Factory
from src.utils.jsonreceiver import JsonReceiver
from src.utils.messages import Payload, PayloadType


class Discovery(JsonReceiver):
    """
    this is both a discovery server and a coin server, the latter is not implemented yet
    """

    def __init__(self, nodes, node_factory=None):
        self.nodes = nodes  # key: vk, val: ip:port
        self.vk = None
        self.addr = None
        self.state = 'SERVER'
        self.factory = node_factory

    def connection_lost(self, reason):
        if self.vk in self.nodes:
            del self.nodes[self.vk]
            print "Discovery: deleted", self.vk

    def json_received(self, msg):
        """
        we don't bother with decoding vk here, since we don't use vk in any crypto functions
        :param msg:
        :return:
        """
        print "Discovery: received", msg

        payload = Payload.from_dict(msg)
        ty = payload.payload_type

        if self.state == 'SERVER':
            if ty == PayloadType.discover.value:
                (_vk, _port) = payload.payload
                self.vk = _vk  # NOTE storing base64 form as is
                self.addr = self.transport.getPeer().host + ":" + str(_port)

                # TODO check addr to be in the form host:port
                if self.vk not in self.nodes:
                    print "Discovery: added node", self.vk, self.addr
                    self.nodes[self.vk] = self.addr

                self.send_json(Payload.make_discover_reply(self.nodes).to_dict())

            elif ty == PayloadType.coin.value:
                raise NotImplementedError

            else:
                print "Discovery: invalid payload type on SERVER", ty
                raise AssertionError

        elif self.state == 'CLIENT':
            if ty == PayloadType.discover_reply.value:
                nodes = payload.payload
                print "Discovery: making new clients...", nodes
                self.factory.new_connection_if_not_exist(nodes)

            elif ty == PayloadType.coin_reply.value:
                raise NotImplementedError

            else:
                raise AssertionError("Discovery: invalid payload type {} on CLIENT".format(ty))

    def say_hello(self, vk, port):
        self.state = 'CLIENT'
        self.send_json(Payload.make_discover((vk, port)).to_dict())
        print "Discovery: discovery sent", vk, port


class DiscoveryFactory(Factory):
    def __init__(self):
        self.nodes = {}  # key = vk, val = addr

    def buildProtocol(self, addr):
        return Discovery(self.nodes)


def got_discovery(p, id, port):
    p.say_hello(id, port)


def run():
    reactor.listenTCP(8123, DiscoveryFactory())
    print "Discovery server running..."
    reactor.run()

if __name__ == '__main__':
    run()
