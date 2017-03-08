from trustchain import *


def test_sigs():
    msg = libnacl.randombytes(8)
    vk, sk = libnacl.crypto_sign_keypair()
    s = Signature(vk, sk, msg)

    # no exception should be thrown
    s.verify(vk)

