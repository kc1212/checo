import random
from messages import Payload
from enum import Enum

Mo14Type = Enum('Mo14Type', 'EST AUX')
Mo14State = Enum('Mo14State', 'stopped start est1 est2 aux coin')


class Mo14:
    def __init__(self, factory):
        self.factory = factory
        self.table = [{}, {}]
        self.est_values = []  # indexed by r, every item is a tuple of sets for 0 and 1, the set contain uuid
        self.aux_values = []  # indexed by r
        self.broadcasted = []  # indexed by r
        self.bin_values = []  # indexed by r, items are binary set()
        self.r = 0
        self.est = -1
        self.state = Mo14State.stopped
        # TODO do the initial broadcast

    def start(self, v):
        assert self.state == Mo14State.stopped  # TODO ?
        assert v in (0, 1)

        self.r += 1
        self.bcast_est(v)
        self.state = Mo14.start
        print "initial message broadcasted", v

    def store_est(self, r, v, uuid):
        pass

    def store_aux(self, r, v, uuid):
        pass

    def handle_bv(self, msg, uuid):
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
        print "received", msg, "from", uuid

        t = self.factory.config.t

        assert(msg["ty"] == Mo14Type.EST.value)
        v = msg["v"]
        assert v in (0, 1)

        self.table[v][uuid] = True

        if len(self.table[v]) >= t + 1 and not self.broadcasted:
            print "relaying", v
            self.bcast_est(v)
            self.broadcasted = True

        if len(self.table[v]) >= 2 * t + 1:
            self.bin_values.add(v)
            print "delivering", v

            if len(self.bin_values) == 2:
                print "found two...", self.bin_values

    def store_msg(self, msg, uuid):
        ty = msg['ty']
        v = msg['v']
        r = msg['r']
        t = self.factory.config.t
        n = self.factory.config.n

        if ty == Mo14Type.EST.value:
            try:
                self.est_values[r][v].add(uuid)
            except ValueError:
                self.est_values.append([set(), set()])
                self.est_values[r][v].add(uuid)
                # we shouldn't be more than 1 round behind

        elif ty == Mo14Type.AUX.value:
            try:
                self.aux_values[r][v].add(uuid)
            except ValueError:
                self.aux_values.append([set(), set()])
                self.aux_values[r][v].add(uuid)
                # we shouldn't be more than 1 round behind

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
        ty = msg['ty']
        v = msg['v']
        r = msg['r']
        t = self.factory.config.t
        n = self.factory.config.n

        self.store_msg(msg, uuid)
        assert r == self.r

        # the state machine, the first broadcast is performed in self.start
        if self.state == Mo14State.start:
            if ty == Mo14Type.AUX.value:
                return
            if len(self.est_values[self.r][v]) >= t + 1 and not self.broadcasted[self.r]:
                print "relaying v", v
                self.bcast_est(v)
                self.broadcasted[self.r] = True
                self.state = Mo14State.est1

        # TODO we need to redo this condition, need to run it more than once for a round
        if self.state == Mo14State.est1:
            if (self.est_values[self.r][v]) >= 2 * t + 1:
                self.bin_values[self.r].add(v)
                self.state = Mo14State.est2

        # we should have something in self.bin_values
        if self.state == Mo14State.est2:
            w = random.choice(self.bin_values[self.r])
            print "relaying w", w
            self.bcast_aux(w)
            self.state = Mo14State.aux

        if self.state == Mo14State.aux:
            vals, count = get_aux_vals(self.aux_values[self.r])
            if count >= n - t:
                if len(vals) == 0 and vals[0] in self.bin_values[self.r]:
                    self.state = Mo14State.coin
                elif len(vals) == 2 and len(self.bin_values[self.r]) == 2:
                    self.state = Mo14State.coin

        if self.state == Mo14State.coin:
            s = 1 # TODO get coin
            if vals == [v]:
                if v == s:
                    print "decided", v
                    self.state = Mo14State.stopped
                else:
                    self.est = v
            else:
                self.est = s

    def bcast_aux(self, v):
        self.bcast(make_aux(self.r, v))

    def bcast_est(self, v):
        self.bcast(make_est(self.r, v))

    def bcast(self, msg):
        print "broadcast:", msg['payload']
        self.factory.bcast(msg)


def get_aux_vals(aux_value):
    """

    :param aux_value:
    :return: (vals, number of aux)
    """
    zero_count = len(aux_value[0])
    one_count = len(aux_value[1])
    length = zero_count + one_count

    if zero_count == 0:
        return [1], length
    if one_count == 0:
        return [0], length
    return [0, 1], length

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

