import math
import libnacl
import pickle  # not the best since it's insecure, but we can't easily use json because it doesn't work with binary

from typing import List, Union
from enum import Enum

ValidityState = Enum('ValidityState', 'Valid Invalid Unknown')


class Signature:
    """
    struct Signature {
        vk: [u8; x], // verification key
        sig: [u8, x],
    }
    """

    def __init__(self, vk, sk, msg):
        # type: (str, str, str) -> None
        self.vk = vk  # this is also the identity
        self.sig = libnacl.crypto_sign(msg, sk)  # self.sig contains the original message

    def verify(self, vk, msg):
        # type: (str, str) -> None
        """
        Throws ValueError on failure
        :return:
        """
        if vk != self.vk:
            raise ValueError("Mismatch verification key")
        if libnacl.crypto_sign_open(self.sig, self.vk) != msg:
            raise ValueError("Mismatch message")

    def dumps(self):
        # type: () -> str
        return pickle.dumps(self)


class TxBlock:
    """
    In the network, TxBlock needs to be created using 3 way handshake.
    1, s -> r: prev, h_s, m
    2, s <- r: prev, h_r, s_r // s seals block
    3, s -> r: s_s // r seals block
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

    class Inner:
        def __init__(self, prev, h_s, h_r, m):
            # type: (str, int, int, str) -> None
            self.prev = prev
            self.h_s = h_s
            self.h_r = h_r
            self.m = m

        def dumps(self):
            # type: () -> str
            return pickle.dumps(self)

    def __init__(self, prev, h_s, h_r, m):
        # type: (str, int, int, str) -> None
        self.inner = self.Inner(prev, h_s, h_r, m)
        self.s_s = None
        self.s_r = None
        self.validity = ValidityState.Unknown

    def sign(self, vk, sk):
        # type: (str, str) -> Signature
        """
        Note that this does not populate the signature field
        :param vk:
        :param sk:
        :return:
        """
        return Signature(vk, sk, self.inner.dumps())

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

        s_r.verify(vk_r, self.make_pair(prev_r).inner.dumps())
        self.s_r = s_r

        s_s.verify(vk_s, self.inner.dumps())
        self.s_s = s_s

        return self

    def make_pair(self, prev):
        # type: (str) -> TxBlock
        """
        Note we reverse h_s and h_r
        :param prev:
        :return: a TxBlock without signatures
        """
        return TxBlock(prev=prev, h_s=self.inner.h_r, h_r=self.inner.h_s, m=self.inner.m)

    def hash(self):
        # type: () -> str
        msg = self.inner.dumps() + self.s_s.dumps() + self.s_r.dumps()
        return libnacl.crypto_hash_sha256(msg)

    def get_h(self):
        # type: () -> int
        return self.inner.h_s

    def get_prev(self):
        # type: () -> str
        return self.inner.prev


class CpBlock:
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

    class Inner:
        def __init__(self, prev, h, cons, ss, p):
            # type: (str, int, Cons, List[Signature], int) -> None
            self.prev = prev
            self.h_s = h
            self.round = cons.round
            self.cons = cons
            self.ss = ss
            self.p = p

        def dumps(self):
            # type: () -> str
            return pickle.dumps(self)

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
        self.inner = self.Inner(prev, h, cons, ss, p)

        if cons.round != -1 or len(ss) != 0 or len(vks) != 0 or self.inner.h_s != 0:
            t = math.floor((len(vks) - 1) / 3.0)
            self._verify_signatures(ss, vks, int(t))
        else:
            # if this is executed, it means this is a genesis block
            pass
        self.s = Signature(vk, sk, self.inner.dumps())

    def hash(self):
        # type: () -> str
        msg = self.inner.dumps() + self.s.dumps()
        return libnacl.crypto_hash_sha256(msg)

    def _verify_signatures(self, ss, vks, t):
        # type: (List[Signature], List[str], int) -> None
        oks = 0
        _ss = [s for s in ss if s.vk in vks]  # only consider nodes that are promoters
        for _s in _ss:
            try:
                _s.verify(_s.vk, self.inner.cons.hash())
                oks += 1
            except ValueError:
                print "verification failed for", _s.vk

        if not oks > t:
            raise ValueError("verification failed, oks = {}, t = {}".format(oks, t))

    def get_h(self):
        # type: () -> int
        return self.inner.h_s

    def get_prev(self):
        # type: () -> str
        return self.inner.prev


class Cons:
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
        self.round = round
        self.blocks = blocks

    def dumps(self):
        # type: () -> str
        return pickle.dumps(self)

    def hash(self):
        # type: () -> str
        return libnacl.crypto_hash_sha256(self.dumps())


def generate_genesis_block(vk, sk):
    # type: (str, str) -> CpBlock
    prev = libnacl.crypto_hash_sha256('0')
    return CpBlock(prev, 0, Cons(-1, []), 0, vk, sk, [], [])


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

    def new_tx(self, tx):
        # type: (TxBlock) -> None
        assert tx.get_prev() == self.chain[-1].hash()

        self.chain.append(tx)

    def new_cp(self, cp):
        # type: (CpBlock) -> None
        assert cp.get_prev() == self.chain[-1].hash()

        prev_cp = self.previous_cp()
        assert prev_cp.inner.round < cp.inner.round

        self.chain.append(cp)

    def previous_cp(self):
        # type: () -> CpBlock
        for b in reversed(self.chain):
            if isinstance(b, CpBlock):
                return b
        raise ValueError("No CpBlock in Chain")


class TrustChain:
    """
    Node maintains one TrustChain object and interacts with it either in in the reactor process or some other process.
    If it's the latter, there needs to be some communication mechanism.

    We assume there's a keyserver, so public keys (vk) of all nodes are available to us.

    type System = Map<Node, Chain>;
    """

    def __init__(self):
        # type: () -> None
        self.sign_vk, self.sign_sk = libnacl.crypto_sign_keypair()
        self.chains = {self.sign_vk: Chain(self.sign_vk, self.sign_sk)}  # HashMap<Node, Chain>
        self.my_chain = self.chains[self.sign_vk]

    def new_tx(self, tx):
        # type: (TxBlock) -> None
        """
        Verify tx, follow the rules and mutates the state to add it
        :return: None
        """
        assert tx.get_h() == len(self.my_chain.chain)
        self.my_chain.new_tx(tx)

    def new_cp(self, cp):
        # type: (CpBlock) -> None
        """
        Verify the cp, follow the rules and mutate the state to add it
        :return: None
        """
        assert cp.get_h() == len(self.my_chain.chain)
        self.my_chain.new_cp(cp)

    def get_h(self):
        # type: () -> int
        return len(self.my_chain.chain)

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
