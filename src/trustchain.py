import libnacl

from enum import Enum
from utils import JsonSerialisable

ValidityState = Enum('ValidityState', 'Valid Invalid Unknown')


class Signature:
    """
    struct Signature {
        vk: [u8; x], // verification key
        sig: [u8, x],
    }
    """
    def __init__(self, vk, sk, msg):
        self.vk = vk
        self.sig = libnacl.crypto_sign(msg, sk)

    def verify(self, vk):
        return libnacl.crypto_sign_open(self.sig, vk)


class TxBlock:
    """
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
    class Inner(JsonSerialisable):
        def __init__(self, prev, h_s, h_r, m):
            self.prev = prev
            self.h_s = h_s
            self.h_r = h_r
            self.m = m

    def __init__(self, prev, h_s, h_r, m):
        self.inner = self.Inner(prev, h_s, h_r, m)
        self.s_s = None
        self.s_r = None
        self.validity = ValidityState.Unknown

    def sign(self, vk, sk, s_r):
        """
        Expect to have obtained s_r from the receiver
        :param vk:
        :param sk:
        :param s_r:
        :return:
        """
        assert self.s_s is None
        assert self.s_r is None
        # TODO verify s_r
        self.s_r = s_r
        self.s_s = Signature(vk, sk, self.inner.to_json())


class CpBlock:
    """
    struct CpBlock {
        prev: Digest,
        round: u64, // of the Cons
        con: Digest, // of the Cons
        p: bool, // promoter registration
        s: Signature,
    }
    """
    class Inner(JsonSerialisable):
        def __init__(self, prev, round, con, p):
            self.prev = prev
            self.round = round
            self.con = con
            self.p = p

    def __init__(self, prev, round, con, p):
        self.inner = self.Inner(prev, round, con, p)
        self.s = None

    def sign(self, vk, sk):
        """
        We expect this function to be called immediately after __init__, when self.s is still None
        :param vk:
        :param sk:
        :return:
        """
        assert self.s is None
        self.s = Signature(vk, sk, self.inner.to_json())
        return self


class Cons:
    """
    struct Cons {
        round: u64,
        blocks: List<CpBlock>,
        ss: List<Signature>,
    }
    """
    def __init__(self, round, blocks, ss):
        self.round = round
        self.blocks = blocks
        self.ss = ss


def generate_genesis_block(vk, sk):
    prev = libnacl.crypto_hash_sha256('0')
    return CpBlock(prev, -1, None, 0).sign(vk, sk)


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
        self.vk = vk
        self.chain = [generate_genesis_block(vk, sk)]

    def new_tx(self, tx):
        pass

    def new_cp(self, cp):
        pass


class TrustChain:
    """
    Node maintains one TrustChain object and interacts with it either in in the reactor process or some other process.
    If it's the latter, there needs to be some communication mechanism.

    type System = Map<Node, Chain>;

    """

    def __init__(self):
        self.sign_vk, self.sign_sk = libnacl.crypto_sign_keypair()
        self.chains = {self.sign_vk: Chain(self.sign_vk, self.sign_sk)}  # HashMap<Node, Chain>

    def new_tx(self, tx):
        """
        Verify tx, follow the rule and mutates the state to add it
        :return: None
        """
        pass

    def new_cp(self, cp):
        """
        Verify the cp, follow the rule a nd mutate the state to add it
        :return: None
        """
        pass

    def pieces(self, tx):
        """
        tx must exist, return the pieces of tx
        :param tx:
        :return: List<Block>
        """
        pass

    def verify(self, tx, resp):
        """

        :param tx:
        :param resp:
        :return:
        """
        pass

    def _enclosure(self, tx):
        """

        :param tx:
        :return: (CpBlock, CpBlock)
        """
        pass