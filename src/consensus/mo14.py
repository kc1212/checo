import logging
import random
from base64 import b64encode
from collections import defaultdict

from enum import Enum
from typing import Union

from src.utils.utils import Replay, Handled
import src.messages.messages_pb2 as pb

_MO14_STATE = Enum('_MO14_STATE', 'stopped start est aux coin')
_EST = pb.Mo14.Type.Value('EST')
_AUX = pb.Mo14.Type.Value('AUX')

_coins = """
01111010 00101101 10001000 10101100 10001101 10011111 10001110 11011111
10010111 01100111 00001000 10101101 11011011 10001101 01110000 01111010
01111000 10111111 11111100 10110111 00010010 00001101 01000101 11010100
11010111 10000000 01000000 11100010 01101011 11111111 00000001 11000110
10111011 01100000 00000010 11001000 01111001 11001000 11011000 11001000
01100111 11011001 01100101 00100110 11001000 01001110 10000000 10100101
00000011 01010010 01000001 10000100 11110000 10010011 00101111 11100100
01010000 01011101 01010000 11000000 10010010 10100100 01110101 00110001
10100101 00001010 10001001 00101011 10111010 00100000 00101001 10001000
11101000 00111011 00101110 01101100 11110011 01101010 01000111 11101110
01100110 00101111 01111100 00011101 01111101 01010000 10000111 00000101
10110100 00010000 11100001 11111110 00101110 01111101 00101100 01110101
10000010 00101001 00101001 01001110 """

# we cheat the common coin...
coins = [int(x) for x in "".join(_coins.split())]


class Mo14(object):
    """
    Mostefaoui et el. '14
    Implemented using a state machine
    """
    def __init__(self, factory, msg_wrapper_f=lambda _x: _x):
        self._factory = factory
        self._r = 0
        self._est = -1
        self._state = _MO14_STATE.start
        self._est_values = {}  # key: r, val: [set(), set()], sets are vk
        self._aux_values = {}  # key: r, val: [set(), set()], sets are vk
        self._broadcasted = defaultdict(bool)  # key: r, val: boolean
        self._bin_values = defaultdict(set)  # key: r, val: binary set()
        self._msg_wrapper_f = msg_wrapper_f

    def start(self, v):
        assert v in (0, 1)

        self._r += 1
        self._bcast_est(v)
        self._state = _MO14_STATE.start
        logging.info("Mo14: initial message broadcasted {}".format(v))

    def _store_msg(self, msg, sender_vk):
        ty = msg.ty
        v = msg.v
        r = msg.r

        if ty == _EST:
            if r not in self._est_values:
                self._est_values[r] = [set(), set()]
            self._est_values[r][v].add(sender_vk)

        elif ty == _AUX:
            if r not in self._aux_values:
                self._aux_values[r] = [set(), set()]
            self._aux_values[r][v].add(sender_vk)

    def handle(self, msg, sender_vk):
        # type: (pb.Mo14, str) -> Union[Handled, Replay]
        """
        We expect messages of type:
        Msg {
            ty: u32,
            r: u32,
            v: u32, // binary
        }
        This function moves the state machine forward when new messages are received
        :param msg: the new messages
        :param sender_vk: verification key of the sender (type str)
        :return:
        """

        # TODO is there a way to start a new round of the algorithm implicitly?

        if self._state == _MO14_STATE.stopped:
            logging.debug("Mo14: not processing due to stopped state")
            return Handled()

        ty = msg.ty
        v = msg.v
        r = msg.r
        t = self._factory.config.t
        n = self._factory.config.n

        logging.debug("Mo14: stored msg (ty: {}, v: {}, r: {}), from {}".format(ty, v, r, b64encode(sender_vk)))
        self._store_msg(msg, sender_vk)

        if r < self._r:
            logging.debug("Mo14: not processing because {} < {}".format(r, self._r))
            return Handled()
        elif r > self._r:
            logging.debug("Mo14: I'm not ready yet {} > {}, the message should be replayed".format(r, self._r))
            return Replay()

        def update_bin_values():
            """
            Main logic of the BV_broadcast algorithm, should be called on every EST message regardless of state
            :return:
            """
            if len(self._est_values[self._r][v]) >= t + 1 and not self._broadcasted[self._r]:
                logging.debug("Mo14: relaying v {}".format(v))
                self._bcast_est(v)
                self._broadcasted[self._r] = True

            if len(self._est_values[self._r][v]) >= 2 * t + 1:
                logging.debug("Mo14: adding to bin_values {}".format(v))
                self._bin_values[self._r].add(v)
                return True

            logging.debug("Mo14: no bin values")
            return False

        if ty == _EST:
            # this condition runs every time EST is received, but the state is updated only at the start state
            got_bin_values = update_bin_values()
            if got_bin_values and self._state == _MO14_STATE.start:
                self._state = _MO14_STATE.est

        if self._state == _MO14_STATE.est:
            # broadcast aux when we have something in bin_values
            logging.debug("Mo14: reached est state")
            w = tuple(self._bin_values[self._r])[0]
            logging.debug("Mo14: relaying w {}".format(w))
            self._bcast_aux(w)
            self._state = _MO14_STATE.aux

        if self._state == _MO14_STATE.aux:
            logging.debug("Mo14: reached aux state")
            if self._r not in self._aux_values:
                logging.debug("Mo14: self.r {} not in self.aux_values {}".format(self._r, self._aux_values))
                return Handled()

            def get_aux_vals(aux_value):
                """

                :param aux_value: [set(), set()], the sets are of vk
                :return: accepted values_i, otherwise None
                """
                if len(self._bin_values[self._r]) == 1:
                    x = tuple(self._bin_values[self._r])[0]
                    if len(aux_value[x]) >= n - t:
                        return set([x])
                elif len(self._bin_values[self._r]) == 2:
                    if len(aux_value[0].union(aux_value[1])) >= n - t:
                        return set([0, 1])
                    elif len(aux_value[0]) >= n - t:
                        return set([0])
                    elif len(aux_value[1]) >= n - t:
                        return set([1])
                    else:
                        logging.debug("Mo14: impossible condition in get_aux_vals")
                        raise AssertionError
                return None

            vals = get_aux_vals(self._aux_values[self._r])
            if vals:
                self._state = _MO14_STATE.coin

        if self._state == _MO14_STATE.coin:
            s = coins[self._r]
            logging.debug("Mo14: reached coin state, s = {}, v = {}".format(s, v))
            logging.debug("Mo14: vals =? set([v]), {} =? {}".format(vals, set([v])))
            if vals == set([v]):
                if v == s:
                    logging.info("Mo14: DECIDED {}".format(v))
                    self._state = _MO14_STATE.stopped
                    return Handled(v)
                else:
                    self._est = v
            else:
                self._est = s

            # start again after round completion
            logging.debug("Mo14: starting again, est = {}".format(self._est))
            self.start(self._est)

        return Handled()

    def _bcast_aux(self, v):
        if self._factory.config.failure == 'byzantine':
            v = random.choice([0, 1])
        assert v in (0, 1)
        logging.debug("Mo14: broadcast aux: v = {}, r = {}".format(v, self._r))
        self._bcast(pb.Mo14(ty=_AUX, r=self._r, v=v))

    def _bcast_est(self, v):
        if self._factory.config.failure == 'byzantine':
            v = random.choice([0, 1])
        assert v in (0, 1)
        logging.debug("Mo14: broadcast est: v = {}, r = {}".format(v, self._r))
        self._bcast(pb.Mo14(ty=_EST, r=self._r, v=v))

    def _bcast(self, msg):
        """
        Broadcasts a Mo14 message, modify the message according to self.msg_wrapper_f
        :param msg:
        :return:
        """
        self._factory.promoter_cast(self._msg_wrapper_f(msg))

