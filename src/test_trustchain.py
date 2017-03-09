import pytest
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


def test_cpblock():
    """
    locally simulate the delivery of cpblock and corresponding signatures
    :return:
    """
    pass
