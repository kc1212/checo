import logging
import random
from base64 import b64encode

import libnacl
from enum import Enum
from pyeclib.ec_iface import ECDriver

import src.messages.messages_pb2 as pb
from src.utils import Handled

_BRACHA_STEP = Enum('_BRACHA_STEP', 'one two three')
_INIT = pb.Bracha.Type.Value('INIT')
_ECHO = pb.Bracha.Type.Value('ECHO')
_READY = pb.Bracha.Type.Value('READY')


class Bracha(object):
    """
    Bracha broadcast '87
    Implemented using state machine (BrachaStep)
    """
    def __init__(self, factory, msg_wrapper_f=lambda _x: _x):
        self._factory = factory
        self._step = _BRACHA_STEP.one
        self._init_count = 0
        self._echo_count = 0
        self._ready_count = 0
        self._root = None
        self._fragments = {}
        self._v = None
        self._done = False
        self._msg_wrapper_f = msg_wrapper_f
        self._n = self._factory.config.n
        self._t = self._factory.config.t
        self._sent_ready = False

        # NOTE: #define EC_MAX_FRAGMENTS 32
        # https://github.com/openstack/liberasurecode/blob/master/include/erasurecode/erasurecode.h
        k = self._n - 2 * self._t
        m = 2 * self._t
        logging.debug("Bracha: erasure code params k={}, m={}".format(k, m))
        self._ec_driver = ECDriver(k=k, m=m, ec_type='liberasurecode_rs_vand')
        random.seed()

    def handle(self, msg, sender_vk):
        # type: (pb.Bracha) -> Handled
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
        if self._done:
            logging.debug("Bracha: done, doing nothing")
            return Handled()

        ty = msg.ty
        logging.debug("Bracha: received {}".format(msg))

        assert isinstance(ty, int)
        assert msg.digest is not None

        # initialisation
        if self._root is None:
            # if body is None, we must be in the initial state
            assert self._init_count == 0 and self._echo_count == 0 and self._ready_count == 0
            self._root = msg.digest

        if self._root != msg.digest:
            logging.debug("Bracha: self.root {} != root: {}, discarding"
                          .format(b64encode(self._root), b64encode(msg.digest)))
            return Handled()

        # here we update the state
        if ty == _INIT:
            self._init_count += 1

        elif ty == _ECHO:
            self._fragments[sender_vk] = msg.fragment
            self._echo_count += 1
            assert self._echo_count == len(self._fragments), \
                "echo_count {} != fragment count {}".format(self._echo_count, len(self._fragments))

        elif ty == _READY:
            self._ready_count += 1
            assert self._root == msg.digest, \
                "self.root {} != root: {}".format(self._root, msg.digest)

        else:
            raise AssertionError("Bracha: unexpected msg type")

        # we should only see one init message
        assert self._init_count == 0 or self._init_count == 1

        # everything below is the algorithm, acting on the current state
        if ty == _INIT:
            logging.debug("Bracha: got init value, root = {}".format(b64encode(msg.digest)))
            self._upon_init(msg)

        if ty == _ECHO:
            logging.debug("Bracha: got echo value, root = {}".format(b64encode(msg.digest)))
            # TODO check Merkle branch
            pass

        if ty == _ECHO and self._echo_count >= self._n - self._t:
            logging.debug("Bracha: got n - t echo values, root = {}".format(b64encode(msg.digest)))
            self._upon_n_minus_t_echo()

        if ty == _READY and self._ready_count >= self._t + 1:
            self._upon_t_plus_1_ready()

        if self._ready_count >= 2 * self._t + 1 and self._echo_count >= self._n - 2*self._t:
            res = self._upon_2t_plus_1_ready()

            # NOTE: we use a random value to trip up tests, since it shouldn't be viewed by tests
            logging.info("Bracha: DELIVER {}"
                         .format(random.random() if self._factory.config.from_instruction else b64encode(res)))

            self._done = True
            return Handled(res)

        return Handled()

    def _upon_init(self, msg):
        assert isinstance(msg, pb.Bracha)
        msg.ty = _ECHO
        self.bcast(msg)

    def _decode_fragments(self):
        fragments = random.sample(self._fragments.values(), self._n - 2 * self._t)
        v = self._ec_driver.decode(fragments)
        return v

    def _upon_n_minus_t_echo(self):
        v = self._decode_fragments()
        assert libnacl.crypto_hash_sha256(v) == self._root
        self._v = v
        logging.debug("Bracha: erasure decoded msg v {}".format(b64encode(v)))

        if not self._sent_ready:
            logging.debug("Bracha: broadcast ready 1, root = {}".format(b64encode(self._root)))
            self.bcast(pb.Bracha(ty=_READY, digest=self._root))
            self._sent_ready = True

    def _upon_t_plus_1_ready(self):
        if not self._sent_ready:
            logging.debug("Bracha: broadcast ready 2, root = {}".format(b64encode(self._root)))
            self.bcast(pb.Bracha(ty=_READY, digest=self._root))
            self._sent_ready = True

    def _upon_2t_plus_1_ready(self):
        if self._v is None:
            self._v = self._decode_fragments()
        return self._v

    def bcast_init(self, msg="some test msg!!"):
        assert isinstance(msg, str)
        self._bcast_init_fragments(msg)

    def _bcast_init_fragments(self, msg):
        """
        
        :param msg: some bytes
        :return: 
        """
        fragments = self._ec_driver.encode(msg)
        digest = libnacl.crypto_hash_sha256(msg)

        logging.info("Bracha: initiate erasure code with {} fragments, digest {}"
                     .format(len(fragments), b64encode(digest)))

        assert len(fragments) == len(self._factory.promoters)
        for fragment, promoter in zip(fragments, self._factory.promoters):
            m = pb.Bracha(ty=_INIT, digest=digest, fragment=fragment)
            self._factory.send(promoter, self._msg_wrapper_f(m))

    def bcast(self, msg):
        self._factory.promoter_cast(self._msg_wrapper_f(msg))
