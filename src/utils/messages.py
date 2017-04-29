from base64 import b64encode
from typing import Union, Dict, List

from src.trustchain.trustchain import Signature, CpBlock, TxBlock, Cons, CompactBlock


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
    def __init__(self, ty, digest, fragment):
        self.ty = ty
        self.digest = digest
        self.fragment = fragment

    def __str__(self):
        return "BrachaMsg - ty: {}, digest: {}, fragment : {}"\
            .format(self.ty, b64encode(self.digest), b64encode(self.fragment))


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
    Simple wrapper around the transaction related messages
    """
    def __init__(self, body):
        # type: (Union[TxReq, TxResp, ValidationReq, ValidationResp]) -> None
        self.body = body


class TxReq:
    def __init__(self, tx):
        # type: (TxBlock) -> None
        self.tx = tx

    def __str__(self):
        return "TxReq - tx: {}".format(self.tx)


class TxResp:
    def __init__(self, seq, tx):
        # type: (int, TxBlock) -> None
        self.seq = seq
        self.tx = tx

    def __str__(self):
        return "TxResp - seq: {}, tx: {}".format(self.seq, self.tx)


class CpMsg:
    def __init__(self, cp):
        # type: (CpBlock) -> None
        self.cp = cp  # type: CpBlock

    @property
    def r(self):
        # type: () -> int
        return self.cp.round


class SigMsg:
    def __init__(self, s, r):
        # type: (Signature, int) -> None
        self.s = s  # type: Signature
        self.r = r  # type: int


class ConsMsg:
    def __init__(self, cons):
        # type: (Cons) -> None
        self.cons = cons  # type: Cons

    @property
    def r(self):
        # type: () -> int
        return self.cons.round


class ValidationReq:
    def __init__(self, seq, seq_r):
        # type: (int) -> None
        self.seq = seq
        self.seq_r = seq_r

    def __str__(self):
        return "ValidationReq - seq: {}, seq_r: {}".format(self.seq, self.seq_r)


class ValidationResp:
    def __init__(self, seq, seq_r, r_a, r_b, pieces):
        # type: (int, int, int, int, List[CompactBlock]) -> None
        self.seq = seq
        self.seq_r = seq_r
        self.r_a = r_a
        self.r_b = r_b
        self.pieces = pieces

    def __str__(self):
        return "ValidationResp - seq: {}, seq_r: {}, r_a: {}, r_b: {}".format(self.seq, self.seq_r, self.r_a, self.r_b)
