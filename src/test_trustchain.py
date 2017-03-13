import pytest
import math
from trustchain import *


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


def test_txblock():
    """
    locally simulate the 3 way handshake
    exceptions are thrown if there are any failure
    :return:
    """
    m, vk_s, sk_s = sigs()
    _, vk_r, sk_r = sigs()

    # s -> r: prev, h_s, m
    s_prev = generate_genesis_block(vk_s, sk_s)
    r_prev = generate_genesis_block(vk_r, sk_r)
    h_s = 1
    h_r = 1

    # r received h_s, h_r so he initialises a TxBlock and creates its signature
    r_block = TxBlock(r_prev.hash(), h_r, h_s, m)
    s_r = r_block.sign(vk_r, sk_r)

    # s <- r: prev, h_r, s_r // s creates block
    s_block = TxBlock(s_prev.hash(), h_s, h_r, m)
    s_s = s_block.sign(vk_s, sk_s)
    s_block.seal(vk_s, s_s, vk_r, s_r, r_prev.hash())

    # s -> r: s_s // r seals block
    r_block.seal(vk_r, s_r, vk_s, s_s, s_prev.hash())


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
    vks = []
    sks = []
    blocks = []
    for _ in range(n):
        _, vk, sk = sigs()
        vks.append(vk)
        sks.append(sk)
        blocks.append(generate_genesis_block(vk, sk))

    # we have n blocks that has reached consensus
    cons = Cons(1, blocks)

    # x of the promoters signed those blocks
    ss = []
    for i, vk, sk in zip(range(x), vks, sks):
        s = Signature(vk, sk, cons.hash())
        ss.append(s)

    # try creating the new checkpoint block
    _, my_vk, my_sk = sigs()
    my_genesis = generate_genesis_block(my_vk, my_sk)

    t = math.floor((n - 1)/3.0)
    if x - 1 >= t:  # number of signatures - 1 is greater than t
        CpBlock(my_genesis.hash(), cons, ss, 1, my_vk, my_sk, vks)
    else:
        with pytest.raises(ValueError):
            CpBlock(my_genesis.hash(), cons, ss, 1, my_vk, my_sk, vks)


