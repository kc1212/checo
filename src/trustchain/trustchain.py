import math
import libnacl
import abc
import copy
import logging
from base64 import b64encode
from typing import List, Union, Dict, Tuple, Optional
from enum import Enum

from src.utils import hash_pointers_ok

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
        # TODO better to hash the packed tuple, use __repr__?
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
            self._sig = None
        else:
            self.vk = vk
            self._sig = libnacl.crypto_sign(msg, sk)

    def __str__(self):
        return '{{"vk": "{}", "sig": "{}"}}'.format(b64encode(self.vk), b64encode(self._sig))

    def _tuple(self):
        return self.vk,  self._sig

    def verify(self, vk, msg):
        # type: (str, str) -> None
        """
        Throws ValueError on failure
        :return:
        """
        if vk != self.vk:
            raise ValueError("Mismatch verification key")
        expected_msg = libnacl.crypto_sign_open(self._sig, self.vk)
        if expected_msg != msg:
            raise ValueError("Mismatch message")


class TxBlockInner(EqHash):
    """
    Ideally this should be defined inside TxBlock, but jsonpickle won't decode correctly if we do that...
    """
    def __init__(self, prev, seq, counterparty, nonce, m):
        # type: (str, int, str, str, str) -> None
        self.prev = prev
        self.seq = seq
        self.counterparty = counterparty
        self.nonce = nonce
        self.m = m

    def _tuple(self):
        return self.prev, self.seq, self.counterparty, self.m


class TxBlock(EqHash):
    def __init__(self, prev, seq, counterparty, m, vk, sk, nonce=None):
        """
        
        :param prev: 
        :param seq: 
        :param counterparty: 
        :param m: 
        :param vk: 
        :param sk: 
        """
        # type: (str, int, str, str, str, str) -> None
        if nonce is None:
            nonce = libnacl.randombytes(32)
        self.inner = TxBlockInner(prev, seq, counterparty, nonce, m)
        self.sig = Signature(vk, sk, self.inner.hash)
        self.other_half = None

        # properties below are used for tracking validation status, not a part of hash
        self.validity = ValidityState.Unknown
        self.request_sent_r = -1  # a positive value indicate the round at which the request is sent

    def __str__(self):
        return '{{"prev": "{}", "seq": {}, "counterparty": "{}", "nonce": "{}", "msg": "{}", "sig": {}}}'\
            .format(b64encode(self.prev),
                    self.inner.seq,
                    b64encode(self.inner.counterparty),
                    b64encode(self.inner.nonce),
                    self.inner.m,
                    self.sig)

    def _tuple(self):
        return self.inner, self.sig

    def to_compact(self):
        # type: () -> CompactBlock
        """
        returns a compact version of this block, the compact version should be used to compute the chain
        :return: 
        """
        return CompactBlock(self.hash, self.prev)

    @property
    def h(self):
        # type: () -> int
        return self.inner.seq

    @property
    def prev(self):
        # type: () -> str
        return self.inner.prev

    def add_other_half(self, other_half):
        # type: (TxBlock) -> ()
        assert self.inner.nonce == other_half.inner.nonce
        assert self.inner.m == other_half.inner.m
        other_half.sig.verify(self.inner.counterparty, other_half.inner.hash)

        self.other_half = other_half


class CpBlockInner(EqHash):
    def __init__(self, prev, seq, cons, ss, p):
        # type: (str, int, Cons, List[Signature], int) -> None
        self.prev = prev
        self.seq = seq
        self.round = cons.round
        self.cons_hash = cons.hash
        self.ss = ss
        self.p = p

    def _tuple(self):
        self.ss.sort(key=lambda x: x.vk)
        return self.prev, self.seq, self.round, self.cons_hash, tuple(self.ss), self.p


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

        if cons.round != -1 or len(ss) != 0 or len(vks) != 0 or self.inner.seq != 0:
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

    def to_compact(self):
        # type: () -> CompactBlock
        return CompactBlock(self.hash, self.prev)

    @property
    def h(self):
        # type: () -> int
        return self.inner.seq

    @property
    def prev(self):
        # type: () -> str
        return self.inner.prev

    @property
    def round(self):
        return self.inner.round


class CompactBlock(EqHash):
    def __init__(self, digest, prev):
        # type: (str, str) -> None
        self.digest = digest
        self.prev = prev

    def _tuple(self):
        return self.digest, self.prev


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
    def __init__(self, vk, sk):
        # type: (str, str) -> None
        self.vk = vk
        self.chain = [generate_genesis_block(vk, sk)]  # type: List[Union[CpBlock, TxBlock]]
        self._tx_count = 0
        self._cp_count = 0

    def new_tx(self, tx):
        # type: (TxBlock) -> None
        assert tx.prev == self.chain[-1].hash
        assert tx.h == self.chain[-1].h + 1

        self.chain.append(tx)
        self._tx_count += 1

    def new_cp(self, cp):
        # type: (CpBlock) -> None
        assert cp.prev == self.chain[-1].hash
        assert cp.h == self.chain[-1].h + 1

        prev_cp = self.latest_cp
        assert prev_cp.inner.round < cp.inner.round, \
            "prev round {}, curr round {}, len {}".format(prev_cp, cp, len(self.chain))

        self.chain.append(cp)
        self._cp_count += 1

    def get_cp_of_round(self, r):
        # TODO an optimisation would be to begin from the back side if 'r' is large
        for block in self.chain:
            if isinstance(block, CpBlock):
                if block.round == r:
                    return block
        return None

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

    def pieces(self, seq):
        # type: (int) -> List[Union[CpBlock, TxBlock]]
        """
        tx must exist, return the pieces of tx
        :param seq:
        :return: List<Block>
        """
        c_a, c_b = self._enclosure(seq)
        if c_a is None or c_b is None:
            return []

        # the height (h) should always be correct, since it is checked when adding new CP
        return self.chain[c_a.h:c_b.h + 1]

    def _enclosure(self, seq):
        # type: (int) -> Tuple[CpBlock, CpBlock]
        """
        Finds two CP blocks that encloses a TX block with a sequence number `seq`
        :param seq: the sequence number of interest, must be a TX block
        :return: (CpBlock, CpBlock)
        """
        tx = self.chain[seq]
        assert isinstance(tx, TxBlock)

        cp_a = cp_b = None

        for i in xrange(seq - 1, -1, -1):
            cp = self.chain[i]
            if isinstance(cp, CpBlock):
                cp_a = cp
                break

        for i in xrange(seq + 1, len(self.chain)):
            cp = self.chain[i]
            if isinstance(cp, CpBlock):
                cp_b = cp
                break

        return cp_a, cp_b

    def set_validity(self, seq, validity):
        # type: (int, ValidityState) -> None
        """
        Set the validity of a tx block, if it's already Valid or Invalid, it cannot be changed.
        :param seq: 
        :param validity: 
        :return: 
        """
        tx = self.chain[seq]
        assert isinstance(tx, TxBlock)
        assert validity != ValidityState.Unknown

        if tx.validity == ValidityState.Unknown:
            tx.validity = validity

    def get_unknown_txs(self):
        """
        Return a list of TXs which have unkonwn validity
        :return: 
        """
        return filter(lambda b: isinstance(b, TxBlock) and b.validity == ValidityState.Unknown, self.chain)

    def get_validated_txs(self):
        """
        Opposite of `get_unknown_txs`
        :return: 
        """
        return filter(lambda b: isinstance(b, TxBlock) and b.validity != ValidityState.Unknown, self.chain)


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
        assert tx.h == self.next_h, "{} != {}".format(tx.h, self.next_h)
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

    @property
    def latest_cp(self):
        # type: () -> CpBlock
        return self.my_chain.latest_cp

    def consensus_round_of_cp(self, cp):
        # type: (CpBlock) -> int
        """
        Given a CP, find the consensus round that contains it
        :param cp: 
        :return: 
        """
        assert isinstance(cp, CpBlock)
        for r in range(cp.round, self.my_chain.latest_round + 1):
            if r in self.consensus:
                if any(map(lambda b: b.hash == cp.hash, self.consensus[r].blocks)):
                    return r
        return -1

    def in_consensus(self, cp, r):
        # type: (CpBlock) -> bool
        """
        Given a CP block and a round, this function checks whether it's in some consensus result
        :param cp: 
        :param r: the round which `cp` is expected to be in
        :return: 
        """
        if r not in self.consensus:
            return False
        cons = self.consensus[r]
        return any(map(lambda b: b.hash == cp.hash and b.round == cp.round, cons.blocks))

    def pieces(self, seq):
        # type: (int) -> List[Union[CpBlock, TxBlock]]
        return self.my_chain.pieces(seq)

    def agreed_pieces(self, seq):
        # type: (int) -> Tuple[List[Union[CpBlock, TxBlock]], int, int]
        c_a, c_b, r_a, r_b = self._agreed_enclosure(seq)
        if c_a is None or c_b is None:
            return [], r_a, r_b

        # the height (h) should always be correct, since it is checked when adding new CP
        return self.my_chain.chain[c_a.h:c_b.h + 1], r_a, r_b

    def _agreed_enclosure(self, seq):
        # type: (int) -> Tuple[Optional[CpBlock], Optional[CpBlock], int, int]
        """
        search backward for an agreed piece
        search forward for an agreed pieces
        get the agreed enclosure
        get the agreed pieces
        :param seq: 
        :return: 
        """

        tx = self.my_chain.chain[seq]
        assert isinstance(tx, TxBlock)

        cp_a = cp_b = None
        r_a = r_b = -1

        for i in xrange(seq - 1, -1, -1):
            cp = self.my_chain.chain[i]
            if isinstance(cp, CpBlock):
                r_a = self.consensus_round_of_cp(cp)
                if r_a != -1:
                    cp_a = cp
                    break

        for i in xrange(seq + 1, len(self.my_chain.chain)):
            cp = self.my_chain.chain[i]
            if isinstance(cp, CpBlock):
                r_b = self.consensus_round_of_cp(cp)
                if r_b != -1:
                    cp_b = cp
                    break

        return cp_a, cp_b, r_a, r_b

    def verify_tx(self, seq, r_a, r_b, resp=None):
        # type: (int, int, int, List[Union[CpBlock, TxBlock]]) -> ValidityState
        """
        We want to verify one of our own TX with expected round numbers that contains the consensus result of the piece
        and the sequence number (height) `seq` that contains the pair
        against some result `resp` we got from the counterparty.
        If the `resp` is empty, we try to use data in the cache (TODO).
        :param seq:
        :param r_a: round which resp[0] is in consensus
        :param r_b: round which resp[-1] is in consensus
        :param resp:
        :return:
        """
        if resp is None:
            raise NotImplemented

        tx = self.my_chain.chain[seq]
        assert isinstance(tx, TxBlock)

        if len(resp) == 0:
            return ValidityState.Unknown

        # check that I also have the same CP blocks
        # hash pointers are ok
        # check the pair is in the received blocks
        # TODO what about the round diff?

        peer_cp_a = resp[0]
        peer_cp_b = resp[-1]
        assert isinstance(peer_cp_a, CpBlock)
        assert isinstance(peer_cp_b, CpBlock)

        if not (self.in_consensus(peer_cp_a, r_a) and self.in_consensus(peer_cp_b, r_b)):
            return ValidityState.Unknown

        if not hash_pointers_ok(resp):
            # we return Unknown here instead of Invalid because the message may be corrupted
            return ValidityState.Unknown

        def contains_h(h, bs):
            return any(map(lambda b: b.h == h, bs))

        if not contains_h(tx.inner.h_r, resp):
            self.my_chain.set_validity(seq, ValidityState.Invalid)
            return ValidityState.Invalid

        self.my_chain.set_validity(seq, ValidityState.Valid)
        return ValidityState.Valid

    def get_unknown_txs(self):
        return self.my_chain.get_unknown_txs()

    def get_validated_txs(self):
        return self.my_chain.get_validated_txs()

# EqHash.register(Signature)
# EqHash.register(TxBlockInner)
# EqHash.register(TxBlock)
# EqHash.register(CpBlockInner)
# EqHash.register(CpBlock)
# EqHash.register(Cons)

# if __name__ == '__main__':
#     vk, sk = libnacl.crypto_sign_keypair()
#     s = Signature(vk, sk, "test")
#     s.verify(vk, "test")
#
#     generate_genesis_block(vk, sk)
