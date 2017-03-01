from enum import Enum

MostefaouiType = Enum('MostefaouiType', 'EST AUX')

class Mostefaoui:
    def __init__(self, config):
        self.config = config
        self.broadcasted = False
        self.table = [{}, {}]
        self.bin_values = set()
        # TODO do the initial broadcast

    def handle(self, msg, uuid):
        """
        Msg {
            ty: u32,
            round: u32,
            body: u32, // binary
        }
        :param msg:
        :return:
        """
        t = self.config.t

        assert(msg["ty"] == MostefaouiType.EST.value)
        v = msg["body"]
        assert v in (0, 1)

        self.table[v][uuid] = True

        if len(self.table[v]) >= t + 1 and not self.broadcasted:
            print "relaying", v
            # TODO do the boradcast
            self.broadcasted = True

        if len(self.table[v]) >= 2 * t + 1:
            self.bin_values.add(v)
            print "delivering", v

            if len(self.bin_values) == 2:
                print "found two...", self.bin_values



