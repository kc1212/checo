import pytest
from typing import Tuple
from src.trustchain import *


@pytest.fixture
def sigs():
    msg = libnacl.randombytes(8)
    vk, sk = libnacl.crypto_sign_keypair()
    return msg, vk, sk


def test_sigs(sigs):
    msg, vk, sk = sigs
    s = Signature(vk, sk, msg)

    # no exception should be thrown
    s.verify(vk, msg)


def test_sigs_failure(sigs):
    msg, vk, sk = sigs
    s = Signature(vk, sk, msg)

    vk, _ = libnacl.crypto_sign_keypair()
    s.vk = vk

    with pytest.raises(ValueError):
        s.verify(vk, msg)


def gen_txblock(prev_s, prev_r, vk_s, sk_s, vk_r, sk_r, h_s, h_r, m):
    # type: (str, str, str, str, str, str, int, int, str) -> Tuple[TxBlock, TxBlock]
    """
    A type signature of strs is just useless
    :param prev_s:
    :param prev_r:
    :param vk_s:
    :param sk_s:
    :param vk_r:
    :param sk_r:
    :param h_s:
    :param h_r:
    :param m:
    :return:
    """
    # r received h_s, h_r so he initialises a TxBlock and creates its signature
    r_block = TxBlock(prev_r, h_r, h_s, m)
    s_r = r_block.sign(vk_r, sk_r)

    # s <- r: prev, h_r, s_r // s creates block
    s_block = TxBlock(prev_s, h_s, h_r, m)
    s_s = s_block.sign(vk_s, sk_s)
    s_block.seal(vk_s, s_s, vk_r, s_r, prev_r)

    # s -> r: s_s // r seals block
    r_block.seal(vk_r, s_r, vk_s, s_s, prev_s)

    return s_block, r_block


def test_txblock():
    """
    locally simulate the 3 way handshake
    exceptions are thrown if there are any failure
    :return:
    """
    m, vk_s, sk_s = sigs()
    _, vk_r, sk_r = sigs()

    # s -> r: prev, h_s, m
    prev_s = generate_genesis_block(vk_s, sk_s).hash
    prev_r = generate_genesis_block(vk_r, sk_r).hash
    h_s = 1
    h_r = 1

    # the following parts of the protocol are covered in gen_txblock
    # r received h_s, h_r so he initialises a TxBlock and creates its signature
    # s <- r: prev, h_r, s_r // s creates block
    # s -> r: s_s // r seals block
    s_block, r_block = gen_txblock(prev_s, prev_r, vk_s, sk_s, vk_r, sk_r, h_s, h_r, m)

    assert s_block.make_pair(prev_r).inner.hash == r_block.inner.hash
    assert r_block.make_pair(prev_s).inner.hash == s_block.inner.hash


@pytest.mark.parametrize("n,x", [
    (4, 1),
    (4, 2),
    (4, 4),
    (19, 6),
    (19, 7),
    (19, 19),
])
def test_cpblock(n, x):
    """
    locally simulate the delivery of cpblock and corresponding signatures
    :return:
    """
    vks, ss, cons = gen_cons(n, 1)
    ss = ss[:x]

    # try creating the new checkpoint block
    _, my_vk, my_sk = sigs()
    my_genesis = generate_genesis_block(my_vk, my_sk)

    t = math.floor((n - 1) / 3.0)
    if x - 1 >= t:  # number of signatures - 1 is greater than t
        CpBlock(my_genesis.hash, 1, cons, 1, my_vk, my_sk, ss, vks)
    else:
        with pytest.raises(ValueError):
            CpBlock(my_genesis.hash, 1, cons, 1, my_vk, my_sk, ss, vks)


def gen_cons(n, cons_round):
    # type: (int) -> Tuple[List[str], List[Signature], Cons]
    """

    :param n:
    :param cons_round:
    :return:
    """
    vks = []
    sks = []
    blocks = []
    for _ in range(n):
        _, vk, sk = sigs()
        vks.append(vk)
        sks.append(sk)
        blocks.append(generate_genesis_block(vk, sk))

    # we have n blocks that has reached consensus
    cons = Cons(cons_round, blocks)

    # x of the promoters signed those blocks
    ss = []
    for i, vk, sk in zip(range(n), vks, sks):
        s = Signature(vk, sk, cons.hash)
        ss.append(s)

    return vks, ss, cons


@pytest.mark.parametrize("n,m", [
    (4, 8),
    (10, 5),
])
def test_cp_chain(n, m):
    """
    Continuously create checkpoint blocks
    :param n: number of nodes
    :param m: number of blocks
    :return:
    """
    _, vk, sk = sigs()
    chain = Chain(vk, sk)
    prev = chain.chain[0].hash

    for i in range(m):
        vks, ss, cons = gen_cons(n, i + 1)
        cp = CpBlock(prev, i + 1, cons, 0, vk, sk, ss, vks)
        prev = cp.hash
        chain.new_cp(cp)

        with pytest.raises(AssertionError):
            # adding again (bad hash) should result in an error
            chain.new_cp(cp)

    assert chain.cp_count == m


@pytest.mark.parametrize("m", [
    4,
])
def test_tx_chain(m):
    """
    I'm making transaction with one other person
    :param m: number of blocks
    :return:
    """
    _, vk_s, sk_s = sigs()
    chain_s = Chain(vk_s, sk_s)
    prev_s = chain_s.chain[0].hash

    _, vk_r, sk_r = sigs()
    chain_r = Chain(vk_r, sk_r)
    prev_r = chain_r.chain[0].hash

    for i in range(m):
        block_s, block_r = gen_txblock(prev_s, prev_r, vk_s, sk_s, vk_r, sk_r, i + 1, i + 1, "test123")
        prev_s = block_s.hash
        prev_r = block_r.hash

        chain_s.new_tx(block_s)
        chain_r.new_tx(block_r)

        assert chain_s.latest_hash == block_s.hash
        assert chain_r.latest_hash == block_r.hash

    assert chain_s.tx_count == m
    assert chain_r.tx_count == m


@pytest.mark.parametrize("n, x, ps", [
    (4, 1, 1),
    (4, 4, 2),
    (4, 4, 4),
    (10, 1, 1),
    (10, 10, 5),
    (10, 10, 10),
])
def test_promoter(n, x, ps):
    vks, ss, cons = gen_cons(n, 1)
    for b, _ in zip(cons.blocks, range(n - ps)):
        b.inner.p = 0

    promoters = cons.get_promoters(x)

    assert len(promoters) == ps
