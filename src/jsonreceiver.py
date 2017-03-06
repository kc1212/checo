import json

from twisted.protocols.basic import LineOnlyReceiver
from utils import byteify


class JsonReceiver(LineOnlyReceiver):
    def connectionLost(self, reason):
        self.connection_lost(reason)

    def lineReceived(self, line):
        obj = byteify(json.loads(line))
        self.json_received(obj)

    def json_received(self, obj):
        # we also expect a dict or list
        raise NotImplementedError

    def connection_lost(self, reason):
        raise NotImplementedError

    def send_json(self, obj):
        # we expect dict or list
        self.sendLine(json.dumps(obj))
