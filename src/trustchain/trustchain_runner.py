from twisted.internet.task import LoopingCall
from twisted.python import log
from base64 import b64encode, b64decode
from Queue import Queue
from enum import Enum
import random

from trustchain import TrustChain, TxBlock, CpBlock
from src.utils.utils import Replay, Handled

MsgType = Enum('MsgType', 'syn synack ack con con_ss')


class TrustChainRunner:
    """
    We keep a queue of messages and handle them in order
    the handle function by itself essentially pushes the messages into the queue
    struct Msg {
        ty: u32,
        body: Syn | SynAck | Ack,
    }

    struct Syn {
        tx_id: u32,
        prev: Digest,  // encode to base64 before sending
        h_s: u32,
        m: String,
    }
    """
    def __init__(self, factory, msg_wrapper_f=lambda x: x):
        self.chain = TrustChain()
        self.factory = factory
        self.tx_locked = False  # only process one transaction at a time, otherwise there'll be hash pointer collisions
        self.recv_q = Queue()
        self.send_q = Queue()  # only for syn messages
        self.msg_wrapper_f = msg_wrapper_f

        self.recv_lc = LoopingCall(self.process_recv_q)
        self.recv_lc.start(0.5).addErrback(log.err)

        self.send_lc = LoopingCall(self.process_send_q)
        self.send_lc.start(0.5).addErrback(log.err)

        # attributes below are temporary, used for negotiating transaction
        self.tx_id = -1  # the id of the transaction that we're processing
        self.block_r = None

        random.seed()

    def handle(self, msg, src):
        print "TC: got message", msg
        self.recv_q.put((msg, src))

    def process_recv_q(self):
        qsize = self.recv_q.qsize()

        cnt = 0
        while not self.recv_q.empty() and cnt < qsize:
            cnt += 1
            msg, src = self.recv_q.get()
            ty = msg['ty']
            body = msg['body']

            if ty == MsgType.syn.value:
                res = self.process_syn(body)
                if isinstance(res, Replay):
                    self.recv_q.put((msg, src))
            elif ty == MsgType.synack.value:
                res = self.process_synack(body)
                if isinstance(res, Replay):
                    self.recv_q.put((msg, src))
            elif ty == MsgType.ack.value:
                self.process_ack(body)
            else:
                raise AssertionError("Incorrect message type {}".format(ty))

    def process_syn(self, msg):
        print "TC: processing syn message", msg
        # put the processing in queue if I'm locked
        if self.tx_locked:
            print "TC: we're locked, putting syn message in queue"
            return Replay()

        # we're not locked, so proceed
        print "TC: not locked, proceeding"
        tx_id = msg['tx_id']
        prev_r = b64decode(msg['prev'])
        h = msg['h']  # height of receiver
        m = msg['m']

        assert self.block_r is None

        self.tx_locked = True
        self.tx_id = tx_id
        self.block_r = TxBlock(prev_r, self.chain.get_h(), h, m)

        self.send_synack()

        return Handled()

    def process_synack(self, msg):
        print "TC: processing synack message", msg
        tx_id = msg['tx_id']
        prev = msg['prev']
        h = msg['h']
        s = msg['s']
        if tx_id != self.tx_id:
            print "TC: not the tx_id we're expecting, putting it back to queue"
            return Replay()

        assert self.tx_locked  # we should be in locked state, because we send the initial syn

        print "TC: processing synack", msg
        # TODO make the block
        self.send_ack()

        return Handled()

    def process_ack(self, msg):
        pass

    def process_send_q(self):
        qsize = self.send_q.qsize()
        cnt = 0
        while not self.send_q.empty() and cnt < qsize:
            if self.tx_locked:
                return

            cnt += 1
            m, node = self.send_q.get()

            tx_id = random.randint(0, 2**31 - 1)
            msg = make_syn(tx_id, b64encode(self.chain.latest_hash()), self.chain.get_h(), m)

            self.tx_locked = True
            self.tx_id = tx_id
            self.send(node, msg)

            print "TC: sent {} to node {}".format(msg, node)

    def send_syn(self, node, m):
        """
        puts the message into the queue for sending on a later time (when we're unlocked)
        we need to do this because we cannot start a transaction at any time, only when we're unlocked
        :param node:
        :param m:
        :return:
        """
        print "TC: putting ({}, {}) in send_q".format(node, m)
        self.send_q.put((m, node))

    def send_synack(self):
        pass

    def send_ack(self):
        pass

    def send(self, node, msg):
        print "TC: sending {} to {}".format(self.msg_wrapper_f(msg), node)
        self.factory.send(node, self.msg_wrapper_f(msg))

    def make_random_tx(self):
        node = random.choice(self.factory.peers.keys())

        # cannot be myself
        while node == self.factory.vk:
            node = random.choice(self.factory.peers.keys())

        m = 'test' + str(random.random())
        print "TC: making random tx to", node
        self.send_syn(node, m)


def make_syn(tx_id, prev, h, m):
    return {'ty': MsgType.syn.value, 'body': {'tx_id': tx_id, 'prev': prev, 'h': h, 'm': m}}


def make_synack(tx_id, prev, h, s):
    return {'ty': MsgType.synack.value, 'body': {'tx_id': tx_id, 'prev': prev, 'h': h, 's': s}}
