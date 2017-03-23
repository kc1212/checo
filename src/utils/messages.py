from typing import Union

from src.trustchain.trustchain import Signature, CpBlock


class DiscoverMsg:
    def __init__(self, vk, port):
        self.vk = vk
        self.port = port


class DiscoverReplyMsg:
    # TODO set the type of nodes
    def __init__(self, nodes):
        self.nodes = nodes


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


class Mo14Msg:
    def __init__(self, ty, r, v):
        self.ty = ty
        self.r = r
        self.v = v


class ACSMsg:
    def __init__(self, instance, round, body):
        # type: (str, int, Union[BrachaMsg, Mo14Msg]) -> None
        self.instance = instance
        self.round = round
        self.body = body  # type: Union[BrachaMsg, Mo14Msg]


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
    def __init__(self, s):
        # type: (Signature) -> None
        self.s = s

