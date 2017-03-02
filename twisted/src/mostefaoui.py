from messages import Payload
from enum import Enum

MostefaouiType = Enum('MostefaouiType', 'EST AUX')

class Mostefaoui:
    def __init__(self, factory):
        self.factory = factory
        self.broadcasted = False
        self.table = [{}, {}]
        self.bin_values = set()
        self.round = -1
        # TODO do the initial broadcast

    def start(self, v):
        self.bcast_est(v)
        print "initial message broadcasted", v

    def handle(self, msg, uuid):
        """
        Msg {
            ty: u32,
            round: u32,
            v: u32, // binary
        }
        :param msg:
        :param uuid:
        :return:
        """
        print "received", msg, "from", uuid

        t = self.factory.config.t

        assert(msg["ty"] == MostefaouiType.EST.value)
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

    def bcast_est(self, v):
        self.bcast(make_est(self.round, v))

    def bcast(self, msg):
        print "broadcast:", msg['payload']
        self.factory.bcast(msg)


def make_est(r, v):
    """
    Make a message of type EST
    :param r: round number
    :param v: binary value
    :return: the message of dict type
    """
    return _make_msg(MostefaouiType.EST.value, r, v)


def _make_msg(ty, r, v):
    return Payload.make_mostefaoui({"ty": ty, "round": r, "v": v}).to_dict()

