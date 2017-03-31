from base64 import b64encode
from typing import Union, Dict

from src.trustchain.trustchain import Signature, CpBlock, Cons


class DiscoverMsg:
    def __init__(self, vk, port):
        self.vk = vk
        self.port = port

    def __str__(self):
        return "DiscoverMsg - vk: {}, port: {}".format(b64encode(self.vk), self.port)


class DiscoverReplyMsg:
    def __init__(self, nodes):
        # type: (Dict[str, str]) -> ()
        self.nodes = nodes

    def __str__(self):
        return "DiscoverReplyMsg - {}".format(self.nodes)


class InstructionMsg:
    def __init__(self, delay, instruction, param=None):
        self.delay = delay
        self.instruction = instruction
        self.param = param

    def __str__(self):
        return "InstructionMsg - delay: {}, instruction: {}, param: {}".format(self.delay, self.instruction, self.param)


class CoinMsg:
    def __init__(self):
        raise NotImplementedError


class CoinReplyMsg:
    def __init__(self):
        raise NotImplementedError


class DummyMsg:
    def __init__(self, m):
        self.m = m


class PingMsg:
    def __init__(self, vk, port):
        self.vk = vk
        self.port = port


class PongMsg:
    def __init__(self, vk, port):
        self.vk = vk
        self.port = port


class BrachaMsg:
    def __init__(self, ty, body):
        self.ty = ty
        self.body = body

    def __str__(self):
        return "BrachaMsg - ty: {}, body: {}".format(self.ty, self.body)


class Mo14Msg:
    def __init__(self, ty, r, v):
        self.ty = ty
        self.r = r
        self.v = v

    def __str__(self):
        return "Mo14Msg - ty: {}, r: {}, v: {}".format(self.ty, self.r, self.v)


class ACSMsg:
    def __init__(self, instance, round, body):
        # type: (str, int, Union[BrachaMsg, Mo14Msg]) -> None
        self.instance = instance
        self.round = round
        self.body = body  # type: Union[BrachaMsg, Mo14Msg]

    def __str__(self):
        return "ACSMsg - instance: {}, round: {}, body: {}".format(b64encode(self.instance), self.round, self.body)


class ChainMsg:
    """
    Simple wrapper around SynMsg, SynAckMsg and AckMsg
    """
    def __init__(self, body):
        # type: (Union[SynMsg, SynAckMsg, AckMsg]) -> None
        self.body = body


class SynMsg:
    def __init__(self, tx_id, prev, h, m):
        # type: (int, str, int, str) -> None
        self.tx_id = tx_id
        self.prev = prev
        self.h = h
        self.m = m


class SynAckMsg:
    def __init__(self, tx_id, prev, h, s):
        # type: (int, str, int, Signature) -> None
        self.tx_id = tx_id
        self.prev = prev
        self.h = h
        self.s = s


class AckMsg:
    def __init__(self, tx_id, s):
        # type: (int, Signature) -> None
        self.tx_id = tx_id
        self.s = s


class CpMsg:
    def __init__(self, cp):
        # type: (CpBlock) -> None
        self.cp = cp


class SigMsg:
    def __init__(self, s, r):
        # type: (Signature, int) -> None
        self.s = s
        self.r = r


class ConsMsg:
    def __init__(self, cons):
        # type: (Cons) -> None
        self.cons = cons

