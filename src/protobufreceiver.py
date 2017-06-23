from struct import pack, unpack

from twisted.protocols.basic import Int32StringReceiver

from google.protobuf.message import Message
import src.messages.messages_pb2 as pb

_PB_PAIRS = [(k, v) for k, v in vars(pb).iteritems() if isinstance(v, type) and issubclass(v, Message)]
_PB_TAG_TO_TUPLE = {_tag: _v for _tag, _v in enumerate(_PB_PAIRS)}
_PB_NAME_TO_TAG = {_v[0]:  _tag for _tag, _v in _PB_TAG_TO_TUPLE.iteritems()}
assert len(_PB_PAIRS) == 21


class ProtobufReceiver(Int32StringReceiver):

    MAX_LENGTH = 20 * 1024 * 1024  # in bytes

    def connectionLost(self, reason):
        self.connection_lost(reason)

    def stringReceived(self, string):
        tag, = unpack("H", string[:2])
        obj = _PB_TAG_TO_TUPLE[tag][1]()
        obj.ParseFromString(string[2:])
        self.obj_received(obj)

    def obj_received(self, obj):
        """
        returns a protobuf Message object
        :param obj: 
        :return: 
        """
        raise NotImplementedError

    def connection_lost(self, reason):
        raise NotImplementedError

    def send_obj(self, obj):
        """
        Note that we use the first encode the object type
        :param obj: 
        :return: 
        """
        msg = pack("H", _PB_NAME_TO_TAG[obj.__class__.__name__]) + obj.SerializeToString()
        self.sendString(msg)

    def lengthLimitExceeded(self, length):
        raise IOError("Line length exceeded, len: {}".format(length))
