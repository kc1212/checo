import random
import logging
import jsonpickle
import libnacl
from base64 import b64encode
from pyeclib.ec_iface import ECDriver
from enum import Enum

from src.utils.messages import BrachaMsg
from src.utils.utils import Handled

BrachaStep = Enum('BrachaStep', 'one two three')
MsgType = Enum('MsgType', 'init echo ready')


class Bracha(object):
    """
    Bracha broadcast '87
    Implemented using state machine (BrachaStep)
    """
    def __init__(self, factory, msg_wrapper_f=lambda _x: _x):
        self.factory = factory
        self.step = BrachaStep.one
        self.init_count = 0
        self.echo_count = 0
        self.ready_count = 0
        self.root = None
        self.fragments = {}
        self.v = None
        self.done = False
        self.msg_wrapper_f = msg_wrapper_f
        self.n = self.factory.config.n
        self.t = self.factory.config.t
        self.sent_ready = False

        # NOTE: #define EC_MAX_FRAGMENTS 32
        # https://github.com/openstack/liberasurecode/blob/master/include/erasurecode/erasurecode.h
        k = self.n - 2 * self.t
        m = 2 * self.t
        logging.debug("Bracha: erasure code params k={}, m={}".format(k, m))
        self.ec_driver = ECDriver(k=k, m=m, ec_type='liberasurecode_rs_vand')
        random.seed()

    def handle(self, msg, sender_vk):
        # type: (BrachaMsg) -> Handled
        """
        This function is called on a new incoming message, we expect the type is correct
        msg should be in the following format
        struct Msg {
            ty: u32,
            body: String,
        }
        :param msg: the input message to send
        :return: the delivered message when completed, otherwise None
        """
        if self.done:
            logging.debug("Bracha: done, doing nothing")
            return Handled()

        ty = msg.ty
        logging.debug("Bracha: received {}".format(msg))

        assert isinstance(ty, int)
        assert msg.digest is not None

        # initialisation
        if self.root is None:
            # if body is None, we must be in the initial state
            assert self.init_count == 0 and self.echo_count == 0 and self.ready_count == 0
            self.root = msg.digest

        if self.root != msg.digest:
            logging.debug("Bracha: self.root {} != root: {}, discarding"
                          .format(b64encode(self.root), b64encode(msg.digest)))
            return Handled()

        # here we update the state
        if ty == MsgType.init.value:
            self.init_count += 1

        elif ty == MsgType.echo.value:
            self.fragments[sender_vk] = msg.fragment
            self.echo_count += 1
            assert self.echo_count == len(self.fragments), \
                "echo_count {} != fragment count {}".format(self.echo_count, len(self.fragments))

        elif ty == MsgType.ready.value:
            self.ready_count += 1
            assert self.root == msg.digest, \
                "self.root {} != root: {}".format(self.root, msg.digest)

        else:
            raise AssertionError("Bracha: unexpected msg type")

        # we should only see one init message
        assert self.init_count == 0 or self.init_count == 1

        # everything below is the algorithm, acting on the current state
        if ty == MsgType.init.value:
            logging.debug("Bracha: got init value, root = {}".format(b64encode(msg.digest)))
            self.upon_init(msg)

        if ty == MsgType.echo.value:
            logging.debug("Bracha: got echo value, root = {}".format(b64encode(msg.digest)))
            # TODO check Merkle branch
            pass

        if ty == MsgType.echo.value and self.echo_count >= self.n - self.t:
            logging.debug("Bracha: got n - t echo values, root = {}".format(b64encode(msg.digest)))
            self.upon_n_minus_t_echo()

        if ty == MsgType.ready.value and self.ready_count >= self.t + 1:
            self.upon_t_plus_1_ready()

        if self.ready_count >= 2 * self.t + 1 and self.echo_count >= self.n - 2*self.t:
            res = self.upon_2t_plus_1_ready()
            log_msg = "{} items of type {}".format(len(res), type(res[0])) if isinstance(res, list) else res
            logging.info("Bracha: DELIVER {}".format(log_msg))
            self.done = True
            return Handled(res)

        return Handled()

    def upon_init(self, msg):
        assert isinstance(msg, BrachaMsg)
        msg.ty = MsgType.echo.value
        self.bcast(msg)

    def decode_fragments(self):
        fragments = random.sample(self.fragments.values(), self.n - 2 * self.t)
        v = self.ec_driver.decode(fragments)
        return v

    def upon_n_minus_t_echo(self):
        v = self.decode_fragments()
        assert libnacl.crypto_hash_sha256(v) == self.root
        self.v = v
        logging.debug("Bracha: erasure decoded msg v {}".format(b64encode(v)))

        if not self.sent_ready:
            logging.debug("Bracha: broadcast ready 1, root = {}".format(b64encode(self.root)))
            self.bcast(BrachaMsg(MsgType.ready.value, self.root, ''))
            self.sent_ready = True

    def upon_t_plus_1_ready(self):
        if not self.sent_ready:
            logging.debug("Bracha: broadcast ready 2, root = {}".format(b64encode(self.root)))
            self.bcast(BrachaMsg(MsgType.ready.value, self.root, ''))
            self.sent_ready = True

    def upon_2t_plus_1_ready(self):
        if self.v is None:
            self.v = self.decode_fragments()
        return jsonpickle.decode(self.v)

    def bcast_init(self, msg="some test msg!!"):
        self.bcast_init_fragments(msg)

    def bcast_init_fragments(self, msg):
        enc_msg = jsonpickle.encode(msg)
        fragments = self.ec_driver.encode(enc_msg)
        digest = libnacl.crypto_hash_sha256(enc_msg)

        log_msg = "{} items of type {}".format(len(msg), type(msg[0])) if isinstance(msg, list) else msg
        logging.info("Bracha: initiate erasure code with {}, digest {}".format(log_msg, b64encode(digest)))

        assert len(fragments) == len(self.factory.promoters)
        for fragment, promoter in zip(fragments, self.factory.promoters):
            msg = BrachaMsg(MsgType.init.value, digest, fragment)
            self.factory.send(promoter, self.msg_wrapper_f(msg))

    def bcast(self, msg):
        self.factory.promoter_cast(self.msg_wrapper_f(msg))
