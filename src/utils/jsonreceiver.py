import jsonpickle

from twisted.protocols.basic import LineOnlyReceiver

from .utils import MAX_LINE_LEN


class JsonReceiver(LineOnlyReceiver):
    def connectionLost(self, reason):
        self.connection_lost(reason)

    def lineReceived(self, line):
        obj = jsonpickle.decode(line)
        self.obj_received(obj)

    def obj_received(self, obj):
        raise NotImplementedError

    def connection_lost(self, reason):
        raise NotImplementedError

    def send_obj(self, obj):
        line = jsonpickle.encode(obj)
        assert len(line) <= MAX_LINE_LEN
        self.sendLine(line)
