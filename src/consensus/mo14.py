import random
from collections import defaultdict
from base64 import b64encode

from enum import Enum
from src.utils.messages import Payload

from src.utils.utils import Replay, Handled

Mo14Type = Enum('Mo14Type', 'EST AUX')
Mo14State = Enum('Mo14State', 'stopped start est aux coin')

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


class Mo14:
    """
    Mostefaoui et el. '14
    Implemented using a state machine
    """
    def __init__(self, factory, acs_hdr_f=lambda _x: _x):
        self.factory = factory
        self.r = 0
        self.est = -1
        self.state = Mo14State.start
        self.est_values = {}  # key: r, val: [set(), set()], sets are vk
        self.aux_values = {}  # key: r, val: [set(), set()], sets are vk
        self.broadcasted = defaultdict(bool)  # key: r, val: boolean
        self.bin_values = defaultdict(set)  # key: r, val: binary set()
        self.acs_hdr_f = acs_hdr_f

    def start(self, v):
        assert v in (0, 1)

        self.r += 1
        self.bcast_est(v)
        self.state = Mo14State.start
        print "Mo14: initial message broadcasted", v

    def store_msg(self, msg, sender_vk):
        ty = msg['ty']
        v = msg['v']
        r = msg['r']

        if ty == Mo14Type.EST.value:
            if r not in self.est_values:
                self.est_values[r] = [set(), set()]
            self.est_values[r][v].add(sender_vk)

        elif ty == Mo14Type.AUX.value:
            if r not in self.aux_values:
                self.aux_values[r] = [set(), set()]
            self.aux_values[r][v].add(sender_vk)

    def handle(self, msg, sender_vk):
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

        if self.state == Mo14State.stopped:
            print "Mo14: not processing due to stopped state"
            return Handled()

        ty = msg['ty']
        v = msg['v']
        r = msg['r']
        t = self.factory.config.t
        n = self.factory.config.n

        print "Mo14: stored msg", msg, b64encode(sender_vk)
        self.store_msg(msg, sender_vk)

        if r < self.r:
            print "Mo14: not processing because {} < {}".format(r, self.r)
            return Handled()
        elif r > self.r:
            print "Mo14: I'm not ready yet {} > {}, the message should be replayed".format(r, self.r)
            return Replay()

        def update_bin_values():
            """
            Main logic of the BV_broadcast algorithm, should be called on every EST message regardless of state
            :return:
            """
            if len(self.est_values[self.r][v]) >= t + 1 and not self.broadcasted[self.r]:
                print "Mo14: relaying v", v
                self.bcast_est(v)
                self.broadcasted[self.r] = True

            if len(self.est_values[self.r][v]) >= 2 * t + 1:
                print "Mo14: adding to bin_values", v
                self.bin_values[self.r].add(v)
                return True

            print "Mo14: no bin values"
            return False

        if ty == Mo14Type.EST.value:
            # this condition runs every time EST is received, but the state is updated only at the start state
            got_bin_values = update_bin_values()
            if got_bin_values and self.state == Mo14State.start:
                self.state = Mo14State.est

        if self.state == Mo14State.est:
            # broadcast aux when we have something in bin_values
            print "Mo14: reached est state"
            w = tuple(self.bin_values[self.r])[0]
            print "Mo14: relaying w", w
            self.bcast_aux(w)
            self.state = Mo14State.aux

        if self.state == Mo14State.aux:
            print "Mo14: reached aux state"
            if self.r not in self.aux_values:
                print "Mo14: self.r not in self.aux_values", self.r, self.aux_values
                return Handled()

            def get_aux_vals(aux_value):
                """

                :param aux_value: [set(), set()], the sets are of vk
                :return: accepted values_i, otherwise None
                """
                if len(self.bin_values[self.r]) == 1:
                    x = tuple(self.bin_values[self.r])[0]
                    if len(aux_value[x]) >= n - t:
                        return set([x])
                elif len(self.bin_values[self.r]) == 2:
                    if len(aux_value[0].union(aux_value[1])) >= n - t:
                        return set([0, 1])
                    elif len(aux_value[0]) >= n - t:
                        return set([0])
                    elif len(aux_value[1]) >= n - t:
                        return set([1])
                    else:
                        print "Mo14: impossible condition in get_aux_vals"
                        raise AssertionError
                return None

            vals = get_aux_vals(self.aux_values[self.r])
            if vals:
                self.state = Mo14State.coin

        if self.state == Mo14State.coin:
            s = coins[self.r]
            print "Mo14: reached coin state, s =", s, "v =", v
            print "Mo14: vals =? set([v])", vals, set([v])
            if vals == set([v]):
                if v == s:
                    print "Mo14: DECIDED", v
                    self.state = Mo14State.stopped
                    return Handled(v)
                else:
                    self.est = v
            else:
                self.est = s

            # start again after round completion
            print "Mo14: starting again, est =", self.est
            self.start(self.est)

        return Handled()

    def bcast_aux(self, v):
        if self.factory.config.failure == 'byzantine':
            v = random.choice([0, 1])
        assert v in (0, 1)
        print "Mo14: broadcast aux:", v, self.r
        self.bcast(make_aux(self.r, v))

    def bcast_est(self, v):
        if self.factory.config.failure == 'byzantine':
            v = random.choice([0, 1])
        assert v in (0, 1)
        print "Mo14: broadcast est:", v, self.r
        self.bcast(make_est(self.r, v))

    def bcast(self, msg):
        """
        Broadcasts a Mo14 message, modify the message according to self.acs_hdr_f
        :param msg:
        :return:
        """
        self.factory.bcast(self.acs_hdr_f(msg))


def make_est(r, v):
    """
    Make a message of type EST
    :param r: round number
    :param v: binary value
    :return: the message of dict type
    """
    return _make_msg(Mo14Type.EST.value, r, v)


def make_aux(r, v):
    return _make_msg(Mo14Type.AUX.value, r, v)


def _make_msg(ty, r, v):
    # TODO need to include ACS header
    return Payload.make_mo14({"ty": ty, "r": r, "v": v}).to_dict()
