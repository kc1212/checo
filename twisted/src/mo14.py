import random
from messages import Payload
from enum import Enum
from collections import defaultdict

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
    def __init__(self, factory):
        self.factory = factory
        self.r = 0
        self.est = -1
        self.state = Mo14State.start
        self.est_values = {}  # key: r, val: [set(), set()], sets are uuid
        self.aux_values = {}  # key: r, val: [set(), set()], sets are uuid
        self.broadcasted = defaultdict(bool)  # key: r, val: boolean
        self.bin_values = defaultdict(set)  # key: r, val: binary set()

    def start(self, v):
        assert v in (0, 1)

        self.r += 1
        self.bcast_est(v)
        self.state = Mo14State.start
        print "initial message broadcasted", v

    def delayed_start(self, v):
        from twisted.internet import reactor
        assert v in (0, 1)

        self.r += 1
        reactor.callLater(5, self.bcast_est, v)
        self.state = Mo14State.start
        print "delayed message broadcasted", v

    def store_msg(self, msg, uuid):
        ty = msg['ty']
        v = msg['v']
        r = msg['r']

        if ty == Mo14Type.EST.value:
            if r not in self.est_values:
                self.est_values[r] = [set(), set()]
            self.est_values[r][v].add(uuid)

        elif ty == Mo14Type.AUX.value:
            if r not in self.aux_values:
                self.aux_values[r] = [set(), set()]
            self.aux_values[r][v].add(uuid)

    def handle(self, msg, uuid):
        """
        Msg {
            ty: u32,
            r: u32,
            v: u32, // binary
        }
        :param msg:
        :param uuid:
        :return:
        """
        # store the message

        if self.state == Mo14State.stopped:
            print "not processing due to stopped state"
            return

        ty = msg['ty']
        v = msg['v']
        r = msg['r']
        t = self.factory.config.t
        n = self.factory.config.n

        self.store_msg(msg, uuid)
        print "stored msg", msg, uuid
        if r != self.r:
            print "not processing because r != self.r", r, self.r
            return

        def update_bin_values():
            if len(self.est_values[self.r][v]) >= t + 1 and not self.broadcasted[self.r]:
                print "relaying v", v
                self.bcast_est(v)
                self.broadcasted[self.r] = True

            if len(self.est_values[self.r][v]) >= 2 * t + 1:
                print "adding to bin_values", v
                self.bin_values[self.r].add(v)
                return True
            return False

        if ty == Mo14Type.EST.value:
            # this condition runs every time EST is received, but the state is updated only at the start state
            got_bin_values = update_bin_values()
            if got_bin_values and self.state == Mo14State.start:
                self.state = Mo14State.est

        if self.state == Mo14State.est:
            # broadcast aux when we have something in bin_values
            print "reached est state"
            w = tuple(self.bin_values[self.r])[0]
            print "relaying w", w
            self.bcast_aux(w)
            self.state = Mo14State.aux

        if self.state == Mo14State.aux:
            print "reached aux state"
            if self.r not in self.aux_values:
                return
            vals, count = get_aux_vals(self.aux_values[self.r])
            if count >= n - t:
                if len(vals) == 1 and tuple(vals)[0] in self.bin_values[self.r]:
                    self.state = Mo14State.coin
                elif len(vals) == 2 and len(self.bin_values[self.r]) == 2:
                    self.state = Mo14State.coin

        if self.state == Mo14State.coin:
            s = coins[self.r]
            print "reached coin state, s =", s, "v =", v
            print "vals =? set([v])", vals, set([v])
            if vals == set([v]):
                if v == s:
                    print "decided on", v, "so we stopped..."
                    self.state = Mo14State.stopped
                    return
                else:
                    self.est = v
            else:
                self.est = s

            # start again after round completion
            self.start(self.est)

    def bcast_aux(self, v):
        assert v in (0, 1)
        print "broadcast aux:", v
        self.bcast(make_aux(self.r, v))

    def bcast_est(self, v):
        assert v in (0, 1)
        print "broadcast est:", v
        self.bcast(make_est(self.r, v))

    def bcast(self, msg):
        self.factory.bcast(msg)


def get_aux_vals(aux_value):
    """

    :param aux_value: [set(), set()], the sets are of uuid
    :return: (vals, tally of vals)
    """
    zero_count = len(aux_value[0])
    one_count = len(aux_value[1])
    length = zero_count + one_count

    if zero_count == 0:
        return set([1]), length
    if one_count == 0:
        return set([0]), length
    return set([0, 1]), length


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
    return Payload.make_mo14({"ty": ty, "r": r, "v": v}).to_dict()

