from twisted.internet.task import LoopingCall
from twisted.python import log
from base64 import b64encode, b64decode
from Queue import Queue
from enum import Enum
import pickle
import random

from trustchain import TrustChain, TxBlock, CpBlock, Signature
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
        self.recv_q = Queue()
        self.send_q = Queue()  # only for syn messages
        self.msg_wrapper_f = msg_wrapper_f

        self.recv_lc = LoopingCall(self.process_recv_q)
        self.recv_lc.start(0.5).addErrback(log.err)

        self.send_lc = LoopingCall(self.process_send_q)
        self.send_lc.start(0.5).addErrback(log.err)

        # attributes below are states used for negotiating transaction
        self.tx_locked = False  # only process one transaction at a time, otherwise there'll be hash pointer collisions
        self.block_r = None
        self.tx_id = -1  # the id of the transaction that we're processing
        self.src = None
        self.s_s = None
        self.m = None
        self.prev_r = None

        random.seed()

    def reset_state(self):
        self.tx_locked = False
        self.tx_id = -1  # the id of the transaction that we're processing
        self.block_r = None
        self.src = None
        self.s_s = None
        self.m = None
        self.prev_r = None

    def assert_unlocked_state(self):
        assert not self.tx_locked
        assert self.block_r is None
        assert self.src is None
        assert self.tx_id == -1
        assert self.s_s is None
        assert self.m is None
        assert self.prev_r is None

    def assert_after_syn_state(self):
        assert self.tx_locked
        assert self.block_r is None
        assert self.src is not None
        assert self.tx_id != -1
        assert self.s_s is None
        assert self.m is not None
        assert self.prev_r is None

    def assert_full_state(self):
        assert self.tx_locked
        assert self.block_r is not None
        assert self.src is not None
        assert self.tx_id != -1
        assert self.s_s is not None
        assert self.m is not None
        assert self.prev_r is not None

    def update_state(self, lock, block, tx_id, src, s_s, m, prev_r):
        self.tx_locked = lock
        self.block_r = block
        self.tx_id = tx_id
        self.src = src
        self.s_s = s_s
        self.m = m
        self.prev_r = prev_r

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
                res = self.process_syn(body, src)
                if isinstance(res, Replay):
                    self.recv_q.put((msg, src))

            elif ty == MsgType.synack.value:
                res = self.process_synack(body, src)
                if isinstance(res, Replay):
                    self.recv_q.put((msg, src))

            elif ty == MsgType.ack.value:
                res = self.process_ack(body, src)
                if isinstance(res, Replay):
                    self.recv_q.put((msg, src))

            else:
                raise AssertionError("Incorrect message type {}".format(ty))

    def process_send_q(self):
        qsize = self.send_q.qsize()
        cnt = 0
        while not self.send_q.empty() and cnt < qsize:
            if self.tx_locked:
                return

            cnt += 1
            m, node = self.send_q.get()

            tx_id = random.randint(0, 2**31 - 1)
            msg = make_syn(tx_id, b64encode(self.chain.latest_hash()), self.chain.next_h(), m)

            self.update_state(True, None, tx_id, node, None, m, None)
            self.send(node, msg)

            print "TC: sent {} to node {}".format(msg, b64encode(node))

    def send_syn(self, node, m):
        """
        puts the message into the queue for sending on a later time (when we're unlocked)
        we need to do this because we cannot start a transaction at any time, only when we're unlocked
        :param node:
        :param m:
        :return:
        """
        print "TC: putting ({}, {}) in send_q".format(b64encode(node), m)
        self.send_q.put((m, node))

    def process_syn(self, msg, src):
        """
        I receive a syn, so I can initiate a block, but cannot seal it (missing signature)
        :param msg: message
        :param src: source/sender of the message
        :return:
        """
        print "TC: processing syn message", msg
        # put the processing in queue if I'm locked
        if self.tx_locked:
            print "TC: we're locked, putting syn message in queue"
            return Replay()

        # we're not locked, so proceed
        print "TC: not locked, proceeding"
        tx_id = msg['tx_id']
        prev_r = b64decode(msg['prev'])
        h_r = msg['h']  # height of receiver
        m = msg['m']

        # make sure we're in the initial state
        self.assert_unlocked_state()
        block = TxBlock(self.chain.latest_hash(), self.chain.next_h(), h_r, m)  # generate s_s from this
        self.update_state(True,
                          block,
                          tx_id,
                          src,
                          block.sign(self.chain.vk, self.chain.sk),  # store my signature
                          m,
                          prev_r)
        self.send_synack()
        return Handled()

    def send_synack(self):
        self.assert_full_state()
        assert not self.block_r.is_sealed()
        msg = make_synack(self.tx_id,
                          b64encode(self.chain.latest_hash()),
                          self.chain.next_h(),
                          self.s_s.to_dict())
        self.send(self.src, msg)

    def process_synack(self, msg, src):
        """
        I should have all the information to make and seal a new tx block
        :param msg:
        :param src:
        :return:
        """
        print "TC: processing synack {} from {}".format(msg, b64encode(src))
        tx_id = msg['tx_id']
        prev_r = b64decode(msg['prev'])
        h_r = msg['h']
        s_r = Signature.from_dict(msg['s'])
        if tx_id != self.tx_id:
            print "TC: not the tx_id we're expecting, putting it back to queue"
            return Replay()

        self.assert_after_syn_state()  # we initiated the syn
        assert src == self.src

        print "TC: synack"
        self.block_r = TxBlock(self.chain.latest_hash(), self.chain.next_h(), h_r, self.m)
        s_s = self.block_r.sign(self.chain.vk, self.chain.sk)
        self.block_r.seal(self.chain.vk, s_s, src, s_r, prev_r)
        self.chain.new_tx(self.block_r)
        print "TC: added tx"

        self.send_ack(s_s)
        return Handled()

    def send_ack(self, s_s):
        msg = make_ack(self.tx_id, s_s.to_dict())
        self.send(self.src, msg)
        self.reset_state()

    def process_ack(self, msg, src):
        print "TC: processing ack {} from {}".format(msg, b64encode(src))
        tx_id = msg['tx_id']
        s_r = Signature.from_dict(msg['s'])
        if tx_id != self.tx_id:
            print "TC: not the tx_id we're expecting, putting it back to queue"
            return Replay()

        assert src == self.src
        assert not self.block_r.is_sealed()

        print "TC: ack"
        self.block_r.seal(self.chain.vk, self.s_s, src, s_r, self.prev_r)
        self.chain.new_tx(self.block_r)
        self.reset_state()
        print "TC: added tx"

        return Handled()

    def send(self, node, msg):
        print "TC: sending {} to {}".format(self.msg_wrapper_f(msg), b64encode(node))
        self.factory.send(node, self.msg_wrapper_f(msg))

    def make_random_tx(self):
        node = random.choice(self.factory.peers.keys())

        # cannot be myself
        while node == self.factory.vk:
            node = random.choice(self.factory.peers.keys())

        m = 'test' + str(random.random())
        print "TC: making random tx to", b64encode(node)
        self.send_syn(node, m)


def make_syn(tx_id, prev, h, m):
    return {'ty': MsgType.syn.value, 'body': {'tx_id': tx_id, 'prev': prev, 'h': h, 'm': m}}


def make_synack(tx_id, prev, h, s):
    return {'ty': MsgType.synack.value, 'body': {'tx_id': tx_id, 'prev': prev, 'h': h, 's': s}}


def make_ack(tx_id, s):
    return {'ty': MsgType.ack.value, 'body': {'tx_id': tx_id, 's': s}}

