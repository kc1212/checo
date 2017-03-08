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


