from twisted.internet import reactor
from twisted.internet.protocol import Factory
from src.utils.jsonreceiver import JsonReceiver
from src.utils.messages import DiscoverMsg, DiscoverReplyMsg, CoinMsg, CoinReplyMsg


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

    def obj_received(self, obj):
        """
        we don't bother with decoding vk here, since we don't use vk in any crypto functions
        :param obj:
        :return:
        """
        print "Discovery: received", obj

        if self.state == 'SERVER':
            if isinstance(obj, DiscoverMsg):
                self.vk = obj.vk  # NOTE storing base64 form as is
                self.addr = self.transport.getPeer().host + ":" + str(obj.port)

                # TODO check addr to be in the form host:port
                if self.vk not in self.nodes:
                    print "Discovery: added node", self.vk, self.addr
                    self.nodes[self.vk] = self.addr

                self.send_obj(DiscoverReplyMsg(self.nodes))

            elif isinstance(obj, CoinMsg):
                raise NotImplementedError

            else:
                raise AssertionError("Discovery: invalid payload type on SERVER")

        elif self.state == 'CLIENT':
            if isinstance(obj, DiscoverReplyMsg):
                print "Discovery: making new clients...", obj.nodes
                self.factory.new_connection_if_not_exist(obj.nodes)

            elif isinstance(obj, CoinReplyMsg):
                raise NotImplementedError

            else:
                raise AssertionError("Discovery: invalid payload type {} on CLIENT".format(ty))

    def say_hello(self, vk, port):
        self.state = 'CLIENT'
        self.send_obj(DiscoverMsg(vk, port))
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
