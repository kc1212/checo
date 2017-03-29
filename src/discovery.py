from twisted.internet import reactor
from twisted.internet.protocol import Factory
from base64 import b64encode
from typing import Union, Dict
import logging

from src.utils.jsonreceiver import JsonReceiver
from src.utils.messages import DiscoverMsg, DiscoverReplyMsg, CoinMsg, CoinReplyMsg
from src.utils.utils import set_logging


class Discovery(JsonReceiver):
    """
    this is both a discovery server and a coin server, the latter is not implemented yet
    """

    def __init__(self, nodes, node_factory=None):
        self.nodes = nodes  # type: Dict[str, str]
        self.vk = None
        self.addr = None
        self.state = 'SERVER'
        self.factory = node_factory

    def connection_lost(self, reason):
        if self.vk in self.nodes:
            del self.nodes[self.vk]
            logging.debug("Discovery: deleted {}".format(b64encode(self.vk)))

    def obj_received(self, obj):
        # type: (Union[DiscoverMsg, DiscoverReplyMsg]) -> None
        """
        we don't bother with decoding vk here, since we don't use vk in any crypto functions
        :param obj:
        :return:
        """
        logging.debug("Discovery: received msg {}".format(obj))

        if self.state == 'SERVER':
            if isinstance(obj, DiscoverMsg):
                self.vk = obj.vk  # NOTE storing base64 form as is
                self.addr = self.transport.getPeer().host + ":" + str(obj.port)

                # TODO check addr to be in the form host:port
                if self.vk not in self.nodes:
                    logging.debug("Discovery: added node {} {}".format(self.vk, self.addr))
                    self.nodes[self.vk] = self.addr

                self.send_obj(DiscoverReplyMsg(self.nodes))

            elif isinstance(obj, CoinMsg):
                raise NotImplementedError

            else:
                raise AssertionError("Discovery: invalid payload type on SERVER")

        elif self.state == 'CLIENT':
            if isinstance(obj, DiscoverReplyMsg):
                logging.debug("Discovery: making new clients...")
                self.factory.new_connection_if_not_exist(obj.nodes)

            elif isinstance(obj, CoinReplyMsg):
                raise NotImplementedError

            else:
                raise AssertionError("Discovery: invalid payload type on CLIENT")

    def say_hello(self, vk, port):
        self.state = 'CLIENT'
        self.send_obj(DiscoverMsg(vk, port))
        logging.debug("Discovery: discovery sent {} {}".format(vk, port))


class DiscoveryFactory(Factory):
    def __init__(self):
        self.nodes = {}  # key = vk, val = addr

    def buildProtocol(self, addr):
        return Discovery(self.nodes)


def got_discovery(p, id, port):
    p.say_hello(id, port)


def run():
    port = 8123
    reactor.listenTCP(port, DiscoveryFactory())
    logging.info("Discovery server running on {}".format(port))
    reactor.run()

if __name__ == '__main__':
    set_logging(logging.DEBUG)
    run()
