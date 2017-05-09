import argparse
import logging

from twisted.internet import reactor, task
from twisted.internet.protocol import Factory
from typing import Union, Dict

from src.messages.messages import DiscoverMsg, DiscoverReplyMsg, CoinMsg, CoinReplyMsg, InstructionMsg
from src.utils.jsonreceiver import JsonReceiver
from src.utils.utils import set_logging, my_err_back, MAX_LINE_LEN, call_later


class Discovery(JsonReceiver):
    """
    this is both a discovery server and a coin server, the latter is not implemented yet
    """

    def __init__(self, nodes, factory):
        self.nodes = nodes  # type: Dict[str, str]
        self.vk = None
        self.addr = None
        self.state = 'SERVER'
        self.factory = factory  # this changes depending on whether it's a server or client

    def connection_lost(self, reason):
        if self.vk in self.nodes:
            del self.nodes[self.vk]
            logging.debug("Discovery: deleted {}".format(self.vk))

    def obj_received(self, obj):
        # type: (Union[DiscoverMsg, DiscoverReplyMsg]) -> None
        """
        we don't bother with decoding vk here, since we don't use vk in any crypto functions
        :param obj:
        :return:
        """
        logging.debug("Discovery: received msg {} from {}".format(obj, self.transport.getPeer().host))

        if self.state == 'SERVER':
            if isinstance(obj, DiscoverMsg):
                self.vk = obj.vk  # NOTE storing base64 form as is
                self.addr = self.transport.getPeer().host + ":" + str(obj.port)

                # TODO check addr to be in the form host:port
                if self.vk not in self.nodes:
                    logging.debug("Discovery: added node {} {}".format(self.vk, self.addr))
                    self.nodes[self.vk] = (self.addr, self)

                assert isinstance(self.factory, DiscoveryFactory)
                self.send_obj(DiscoverReplyMsg(self.factory.make_nodes_msg()))

            elif isinstance(obj, CoinMsg):
                raise NotImplementedError

            else:
                raise AssertionError("Discovery: invalid payload type on SERVER")

        elif self.state == 'CLIENT':
            if isinstance(obj, DiscoverReplyMsg):
                logging.debug("Discovery: making new clients...")
                self.factory.new_connection_if_not_exist(obj.nodes)

            elif isinstance(obj, InstructionMsg):
                self.factory.handle_instruction(obj)

            elif isinstance(obj, CoinReplyMsg):
                raise NotImplementedError

            else:
                raise AssertionError("Discovery: invalid payload type on CLIENT")

    def say_hello(self, vk, port):
        self.state = 'CLIENT'
        self.send_obj(DiscoverMsg(vk, port))
        logging.debug("Discovery: discovery sent {} {}".format(vk, port))


class DiscoveryFactory(Factory):
    def __init__(self, n, t, m, inst):
        self.nodes = {}  # key = vk, val = addr

        def has_sufficient_instruction_params():
            return n is not None and \
                   t is not None and \
                   m is not None and \
                   inst is not None and \
                   len(inst) >= 2

        if has_sufficient_instruction_params():
            logging.info("Sufficient params to send instructions")
            self.n = n
            self.t = t
            self.m = m

            self.inst_delay = int(inst[0])
            self.inst_inst = inst[1]
            self.inst_param = None if len(inst) < 3 else inst[2]

            self.lc = task.LoopingCall(self.send_instruction_when_ready)
            self.lc.start(5).addErrback(my_err_back)
            self.sent = False

            def exit_if_not_sent():
                if not self.sent:
                    raise AssertionError("not sent")
            call_later(120, exit_if_not_sent)

        else:
            logging.info("Insufficient params to send instructions")

    def make_nodes_msg(self):
        msg = {k: v[0] for k, v in self.nodes.iteritems()}
        return msg

    def send_instruction_when_ready(self):
        if len(self.nodes) >= self.m:
            msg = InstructionMsg(self.inst_delay, self.inst_inst, self.inst_param)
            logging.debug("Broadcasting instruction {}".format(msg))
            self.bcast(msg)
            self.sent = True
            self.lc.stop()
        else:
            logging.debug("Instruction not ready ({} / {})...".format(len(self.nodes), self.m))

    def buildProtocol(self, addr):
        return Discovery(self.nodes, self)

    def bcast(self, msg):
        for k, v in self.nodes.iteritems():
            proto = v[1]
            proto.send_obj(msg)


def got_discovery(p, id, port):
    p.say_hello(id, port)


def run(port, n, t, m, inst):
    JsonReceiver.MAX_LENGTH = MAX_LINE_LEN
    reactor.listenTCP(port, DiscoveryFactory(n, t, m, inst))
    logging.info("Discovery server running on {}".format(port))
    reactor.run()

if __name__ == '__main__':
    set_logging(logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--port',
        type=int, help='the listener port, default 8123',
        default=8123,
    )
    parser.add_argument(
        '-n',
        type=int, help='the total number of promoters',
        nargs='?'
    )
    parser.add_argument(
        '-t',
        type=int, help='the total number of malicious nodes',
        nargs='?'
    )
    parser.add_argument(
        '-m',
        type=int, help='the total number of nodes',
        nargs='?'
    )
    parser.add_argument(
        '--inst',
        metavar='INST',
        help='the instruction to send after all nodes are connected',
        nargs='*'
    )
    args = parser.parse_args()

    # NOTE: n, t, m and inst must be all or nothing
    run(args.port, args.n, args.t, args.m, args.inst)
