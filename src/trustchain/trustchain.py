import math
import libnacl
import abc
import copy
import logging
from base64 import b64encode
from typing import List, Union, Dict
from enum import Enum


ValidityState = Enum('ValidityState', 'Valid Invalid Unknown')


class EqHash:
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def _tuple(self):
        """Please implement me"""
        raise NotImplementedError

    def __eq__(self, other):
        return self._tuple() == other._tuple()

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self._tuple())

    @property
    def hash(self):
        return libnacl.crypto_hash_sha256(str(self.__hash__()))


class Signature(EqHash):
    """
    Data structure stores the verification key along with the signature,
    we expect the original message to be small, preferably a digest
    """
    def __init__(self, vk=None, sk=None, msg=None):
        # type: (str, str, str) -> None

        if vk is None and sk is None and msg is None:
            self.vk = None
            self.sig = None
        else:
            self.vk = vk
            self.sig = libnacl.crypto_sign(msg, sk)

    def __str__(self):
        return '{{"vk": "{}", "sig": "{}"}}'.format(b64encode(self.vk), b64encode(self.sig))

    def _tuple(self):
        return self.vk,  self.sig

    def verify(self, vk, msg):
        # type: (str, str) -> None
        """
        Throws ValueError on failure
        :return:
        """
        if vk != self.vk:
            raise ValueError("Mismatch verification key")
        expected_msg = libnacl.crypto_sign_open(self.sig, self.vk)
        if expected_msg != msg:
            raise ValueError("Mismatch message")


class TxBlockInner(EqHash):
    """
    Ideally this should be defined inside TxBlock, but jsonpickle won't decode correctly if we do that...
    """
    def __init__(self, prev, h_s, h_r, m):
        # type: (str, int, int, str) -> None
        self.prev = prev
        self.h_s = h_s
        self.h_r = h_r
        self.m = m

    def _tuple(self):
        return self.prev, self.h_s, self.h_r, self.m


class TxBlock(EqHash):
    """
    In the network, TxBlock needs to be created using 3 way handshake.
    1, s -> r: prev, h_s, m // syn
    2, s <- r: prev, h_r, s_r // synack, s seals block
    3, s -> r: s_s // ack, r seals block
    I'm always the sender, regardless of who initialised the handshake.
    This protocol is not Byzantine fault tolerant, ongoing work for "double signature"
    struct TxBlock {
        prev: Digest,
        h_s: u64,
        h_r: u64,
        s_s: Signature,
        s_r: Signature,
        m: String,

        // items below are not a part of the block digest
        validity: Valid | Invalid | Unknown
    }
    """
    def __init__(self, prev, h_s, h_r, m):
        # type: (str, int, int, str) -> None
        self.inner = TxBlockInner(prev, h_s, h_r, m)
        self.s_s = None
        self.s_r = None
        self.validity = ValidityState.Unknown

    def __str__(self):
        return '{{"prev": "{}", "h_s": {}, "h_r": {}, "s_s": {}, "s_r": {}, "m": "{}"}}'\
            .format(b64encode(self.prev),
                    self.inner.h_s, self.inner.h_r,
                    self.s_s, self.s_r, self.inner.m)

    def _tuple(self):
        return self.inner, self.s_s, self.s_r, self.validity

    def sign(self, vk, sk):
        # type: (str, str) -> Signature
        """
        Note that this does not populate the signature field
        :param vk:
        :param sk:
        :return:
        """
        return Signature(vk, sk, self.inner.hash)

    def seal(self, vk_s, s_s, vk_r, s_r, prev_r):
        # type: (str, Signature, str, Signature, str) -> TxBlock
        """
        Expect to have obtained s_r from the receiver
        :param vk_s:
        :param s_s:
        :param vk_r: receiver verification key
        :param s_r: the previous digest of the receiver
        :param prev_r:
        :return:
        """
        assert self.s_s is None
        assert self.s_r is None

        s_r.verify(vk_r, self.make_pair(prev_r).inner.hash)
        self.s_r = s_r

        s_s.verify(vk_s, self.inner.hash)
        self.s_s = s_s

        return self

    def is_sealed(self):
        if self.s_s is None or self.s_r is None:
            return False
        return True

    def make_pair(self, prev):
        # type: (str) -> TxBlock
        """
        Note we reverse h_s and h_r
        :param prev:
        :return: a TxBlock without signatures
        """
        return TxBlock(prev=prev, h_s=self.inner.h_r, h_r=self.inner.h_s, m=self.inner.m)

    @property
    def h(self):
        # type: () -> int
        return self.inner.h_s

    @property
    def prev(self):
        # type: () -> str
        return self.inner.prev


class CpBlockInner(EqHash):
    def __init__(self, prev, h, cons, ss, p):
        # type: (str, int, Cons, List[Signature], int) -> None
        self.prev = prev
        self.h_s = h
        self.round = cons.round
        self.cons_hash = cons.hash
        self.ss = ss
        self.p = p

    def _tuple(self):
        self.ss.sort(key=lambda x: x.vk)
        return self.prev, self.h_s, self.round, self.cons_hash, tuple(self.ss), self.p


class CpBlock(EqHash):
    """
    1, node receives some consensus result
    2, node receives some signatures
    3, generate the cp block
    struct CpBlock {
        prev: Digest,
        round: u64, // of the Cons
        con: Digest, // of the Cons
        p: bool, // promoter registration
        s: Signature,
    }
    """

    def __init__(self, prev, h, cons, p, vk, sk, ss, vks):
        # type: (str, int, Cons, int, str, str, List[Signature], List[str]) -> None
        """

        :param prev: hash pointer to the previous block
        :param cons: type Cons
        :param h: height
        :param p: promoter flag
        :param vk: my verification key
        :param sk: my secret key
        :param ss: signatures of the promoters, at least t-1 of them must be valid
        :param vks: all verification keys of promoters
        """
        assert p in (0, 1)
        self.inner = CpBlockInner(prev, h, cons, ss, p)

        if cons.round != -1 or len(ss) != 0 or len(vks) != 0 or self.inner.h_s != 0:
            t = math.floor((len(vks) - 1) / 3.0)
            self._verify_signatures(ss, vks, int(t))
        else:
            # if this is executed, it means this is a genesis block
            pass
        self.s = Signature(vk, sk, self.inner.hash)

    def __str__(self):
        return '{{"prev": "{}", "cons": "{}", "h": {}, "r": {}, "p": {}, "s": {}}}'\
            .format(b64encode(self.prev), b64encode(self.inner.cons_hash),
                    self.h, self.inner.round, self.inner.p, self.s)

    def _tuple(self):
        return self.inner, self.s

    @property
    def luck(self):
        return libnacl.crypto_hash_sha256(self.hash + self.s.vk)

    def _verify_signatures(self, ss, vks, t):
        # type: (List[Signature], List[str], int) -> None
        oks = 0
        _ss = [s for s in ss if s.vk in vks]  # only consider nodes that are promoters
        for _s in _ss:
            try:
                _s.verify(_s.vk, self.inner.cons_hash)
                oks += 1
            except ValueError:
                logging.debug("one verification failed for {}".format(_s.vk))

        if not oks > t:
            raise ValueError("verification failed, oks = {}, t = {}".format(oks, t))

    @property
    def h(self):
        # type: () -> int
        return self.inner.h_s

    @property
    def prev(self):
        # type: () -> str
        return self.inner.prev

    @property
    def round(self):
        return self.inner.round


class Cons(EqHash):
    """
    The consensus results, data structure that the promoters agree on
    struct Cons {
        round: u64,
        blocks: List<CpBlock>,
    }
    """

    def __init__(self, round, blocks):
        # type: (int, List[CpBlock]) -> None
        """

        :param round: consensus round
        :param blocks: list of agreed checkpoint blocks
        """
        # assert len(blocks) > 0
        # assert isinstance(blocks[0], CpBlock)
        self.round = round
        self.blocks = blocks

    def __str__(self):
        return '{{"r": {}, "blocks": {}}}'.format(self.round, len(self.blocks))

    def _tuple(self):
        # TODO sorting this every time may be inefficient...
        self.blocks.sort(key=lambda x: hash(x))
        return (self.round,) + tuple(self.blocks)

    def get_promoters(self, n):
        # type: () -> List[str]
        registered = filter(lambda cp: cp.inner.p == 1, self.blocks)
        registered.sort(key=lambda x: x.luck)
        return [b.s.vk for b in registered][:n]

    @property
    def count(self):
        # type: () -> int
        return len(self.blocks)


def generate_genesis_block(vk, sk):
    # type: (str, str) -> CpBlock
    prev = libnacl.crypto_hash_sha256('0')
    return CpBlock(prev, 0, Cons(0, []), 1, vk, sk, [], [])


class Chain:
    """
    enum Block {
        TxBlock,
        CpBlock,
    }

    // height (sequence number) should match the index
    type Chain = List<Block>;
    """

    def __init__(self, vk, sk):
        # type: (str, str) -> None
        self.vk = vk
        self.chain = [generate_genesis_block(vk, sk)]  # type: List[Union[CpBlock, TxBlock]]
        self._tx_count = 0
        self._cp_count = 0

    def new_tx(self, tx):
        # type: (TxBlock) -> None
        assert tx.prev == self.chain[-1].hash

        self.chain.append(tx)
        self._tx_count += 1

    def new_cp(self, cp):
        # type: (CpBlock) -> None
        assert cp.prev == self.chain[-1].hash

        prev_cp = self.latest_cp
        assert prev_cp.inner.round < cp.inner.round, \
            "prev round {}, curr round {}, len {}".format(prev_cp, cp, len(self.chain))

        self.chain.append(cp)
        self._cp_count += 1

    @property
    def latest_hash(self):
        # type: () -> str
        return self.chain[-1].hash

    @property
    def latest_cp(self):
        # type: () -> CpBlock
        for b in reversed(self.chain):
            if isinstance(b, CpBlock):
                return b
        raise ValueError("No CpBlock in Chain")

    @property
    def genesis(self):
        # type: () -> CpBlock
        return self.chain[0]

    @property
    def latest_round(self):
        # type: () -> int
        return self.latest_cp.inner.round

    @property
    def tx_count(self):
        # type: () -> int
        return self._tx_count

    @property
    def cp_count(self):
        # type: () -> int
        return self._cp_count


class TrustChain:
    """
    Node maintains one TrustChain object and interacts with it either in in the reactor process or some other process.
    If it's the latter, there needs to be some communication mechanism.

    We assume there's a keyserver, so public keys (vk) of all nodes are available to us.

    type System = Map<Node, Chain>;
    """

    def __init__(self):
        # type: () -> None
        self.vk, self.sk = libnacl.crypto_sign_keypair()
        self.chains = {self.vk: Chain(self.vk, self.sk)}  # type: Dict[str, Chain]
        self.my_chain = self.chains[self.vk]
        self.consensus = {}  # type: Dict[int, Cons]
        logging.info("TC: my VK is {}".format(b64encode(self.vk)))

    def new_tx(self, tx):
        # type: (TxBlock) -> None
        """
        Verify tx, follow the rules and mutates the state to add it
        :return: None
        """
        assert tx.h == self.next_h
        self.my_chain.new_tx(copy.deepcopy(tx))

    def new_cp(self, p, cons, ss, vks):
        # type: (int, Cons, List[Signature], List[str], List[str]) -> None
        """

        :param p:
        :param cons:
        :param ss: signature of the promoters
        :param vks: verification key of the promoters
        :return:
        """
        assert cons.round not in self.consensus
        self.consensus[cons.round] = cons
        cp = CpBlock(self.latest_hash, self.next_h, cons, p, self.vk, self.sk, ss, vks)
        self._new_cp(cp)

    def _new_cp(self, cp):
        # type: (CpBlock) -> None
        """
        Verify the cp, follow the rules and mutate the state to add it
        NOTE: this does not cache the consensus result
        :return: None
        """
        assert cp.h == len(self.my_chain.chain)
        self.my_chain.new_cp(copy.deepcopy(cp))

    @property
    def next_h(self):
        # type: () -> int
        return len(self.my_chain.chain)

    @property
    def latest_hash(self):
        # type () -> str
        return self.my_chain.latest_hash

    @property
    def genesis(self):
        # type () -> CpBlock
        return self.my_chain.genesis

    @property
    def latest_round(self):
        # type: () -> int
        return self.my_chain.latest_round

    @property
    def tx_count(self):
        # type: () -> int
        return self.my_chain.tx_count

    @property
    def cp_count(self):
        # type: () -> int
        return self.my_chain.cp_count

    def pieces(self, tx):
        """
        tx must exist, return the pieces of tx
        :param tx:
        :return: List<Block>
        """
        raise NotImplementedError

    def verify(self, tx, resp):
        """

        :param tx:
        :param resp:
        :return:
        """
        raise NotImplementedError

    def _enclosure(self, tx):
        """

        :param tx:
        :return: (CpBlock, CpBlock)
        """
        raise NotImplementedError


# EqHash.register(Signature)
# EqHash.register(TxBlockInner)
# EqHash.register(TxBlock)
# EqHash.register(CpBlockInner)
# EqHash.register(CpBlock)
# EqHash.register(Cons)

if __name__ == '__main__':
    vk, sk = libnacl.crypto_sign_keypair()
    s = Signature(vk, sk, "test")
    s.verify(vk, "test")

    generate_genesis_block(vk, sk)
