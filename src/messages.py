import json
from enum import Enum

PayloadType = Enum('PayloadType', 'discover discover_reply ping pong bracha mo14 coin coin_reply acs dummy')


class Payload:
    def __init__(self, ty, payload):
        assert isinstance(ty, int)
        self.payload_type = ty
        self.payload = payload

    @classmethod
    def dummy(cls):
        return cls(-1, "dummy")

    @classmethod
    def make_ping(cls, payload):
        return cls(PayloadType.ping.value, payload)

    @classmethod
    def make_pong(cls, payload):
        return cls(PayloadType.pong.value, payload)

    @classmethod
    def make_dummy(cls, payload):
        return cls(PayloadType.dummy.value, payload)

    @classmethod
    def make_bracha(cls, payload):
        return cls(PayloadType.bracha.value, payload)

    @classmethod
    def make_mo14(cls, payload):
        return cls(PayloadType.mo14.value, payload)

    @classmethod
    def make_coin(cls, payload):
        return cls(PayloadType.coin.value, payload)

    @classmethod
    def make_discover(cls, payload):
        return cls(PayloadType.discover.value, payload)

    @classmethod
    def make_discover_reply(cls, payload):
        return cls(PayloadType.discover_reply.value, payload)

    @classmethod
    def make_acs(cls, payload):
        return cls(PayloadType.acs.value, payload)

    @classmethod
    def from_dict(cls, d):
        return cls(d["payload_type"], d["payload"])

    def from_json(self, j):
        self.__dict__ = json.loads(j)
        assert isinstance(self.payload_type, int)
        return self

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__)

    def to_dict(self):
        return self.__dict__


if __name__ == '__main__':
    p1 = Payload(1, "asdf")
    print p1
    print p1.to_json()

    p2 = Payload.dummy().from_json('{"payload_type": 0, "payload": "zzz"}')
    print p2
    print p2.to_json()
