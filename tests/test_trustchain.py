import pytest
from src.trustchain import *
from src.utils import hash_pointers_ok


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
    tx_s = TxBlock(prev_s, h_s, vk_r, m, vk_s, sk_s)
    tx_r = TxBlock(prev_r, h_r, vk_s, m, vk_r, sk_r, tx_s.inner.nonce)

    tx_s.add_other_half(tx_r)
    tx_r.add_other_half(tx_s)

    return tx_s, tx_r


def test_txblock():
    """
    test creation of a single block
    :return:
    """
    m, vk_s, sk_s = sigs()
    _, vk_r, sk_r = sigs()

    prev_s = generate_genesis_block(vk_s, sk_s).compact.hash
    prev_r = generate_genesis_block(vk_r, sk_r).compact.hash
    h_s = 1
    h_r = 1

    # assertions are mostly in `add_other_half`
    gen_txblock(prev_s, prev_r, vk_s, sk_s, vk_r, sk_r, h_s, h_r, m)


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

    t = (n - 1) / 3
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
    prev = chain.chain[0].compact.hash

    for i in range(m):
        vks, ss, cons = gen_cons(n, i + 1)
        cp = CpBlock(prev, i + 1, cons, 0, vk, sk, ss, vks)
        prev = cp.compact.hash
        chain.new_cp(cp)

        with pytest.raises(AssertionError):
            # adding again (bad hash) should result in an error
            chain.new_cp(cp)

    assert chain.cp_count == m
    assert hash_pointers_ok([b.compact for b in chain.chain])


@pytest.mark.parametrize("m", [
    4,
    8,
])
def test_tx_chain(m):
    """
    I'm making transaction with one other person
    :param m: number of blocks
    :return:
    """
    _, vk_s, sk_s = sigs()
    chain_s = Chain(vk_s, sk_s)
    prev_s = chain_s.chain[0].compact.hash

    _, vk_r, sk_r = sigs()
    chain_r = Chain(vk_r, sk_r)
    prev_r = chain_r.chain[0].compact.hash

    for i in range(m):
        block_s, block_r = gen_txblock(prev_s, prev_r, vk_s, sk_s, vk_r, sk_r, i + 1, i + 1, "test123")
        prev_s = block_s.compact.hash
        prev_r = block_r.compact.hash

        chain_s.new_tx(block_s)
        chain_r.new_tx(block_r)

        assert chain_s.latest_compact_hash == block_s.compact.hash
        assert chain_r.latest_compact_hash == block_r.compact.hash

    assert chain_s.tx_count == m
    assert chain_r.tx_count == m

    assert hash_pointers_ok([b.compact for b in chain_s.chain])
    assert hash_pointers_ok([b.compact for b in chain_r.chain])


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


def generate_tc_pair(n_cp, n_tx):
    """
    
    :param n_cp: number of CP blocks excluding the genesis block
    :param n_tx: number of TX blocks in between CP blocks
    :return: 
    """
    tc_s = TrustChain()
    vk_s = tc_s.vk
    sk_s = tc_s._sk

    tc_r = TrustChain()
    vk_r = tc_r.vk
    sk_r = tc_r._sk

    vks = [vk_s, vk_r]

    for i in range(n_cp):
        for j in range(n_tx):
            tx_s, tx_r = gen_txblock(tc_s.latest_compact_hash, tc_r.latest_compact_hash,
                                     vk_s, sk_s, vk_r, sk_r,
                                     tc_s.next_seq, tc_r.next_seq, "123test")
            tc_s._new_tx(tx_s)
            tc_r._new_tx(tx_r)

        r = i + 1
        cons = Cons(r, [tc_s.latest_cp, tc_r.latest_cp])
        ss = [Signature(vk_s, sk_s, cons.hash), Signature(vk_r, sk_r, cons.hash)]

        tc_s.new_cp(1, cons, ss, vks)
        tc_r.new_cp(1, cons, ss, vks)

    assert tc_r.my_chain.tx_count == n_cp * n_tx
    assert tc_r.my_chain.cp_count == n_cp

    assert tc_s.my_chain.tx_count == n_cp * n_tx
    assert tc_s.my_chain.cp_count == n_cp

    return tc_s, tc_r


@pytest.mark.parametrize("seq,n_cp,n_tx", [
    (4, 3, 5),
    (7, 3, 5),
])
def test_pieces(seq, n_cp, n_tx):
    tc_s, tc_r = generate_tc_pair(n_cp, n_tx)
    pieces = tc_s.my_chain.pieces(seq)
    pieces2 = tc_s.agreed_pieces(seq)

    # note that this only compares the parts in _tuple
    assert pieces == pieces2
    assert hash_pointers_ok(pieces)


@pytest.mark.parametrize("seq,n_cp,n_tx,expected", [
    (4, 3, 5, ValidityState.Valid),
    (7, 3, 5, ValidityState.Valid),
    (15, 3, 5, ValidityState.Unknown)
])
def test_validation(seq, n_cp, n_tx, expected):
    """
    
    :param seq: 
    :param n_cp: 
    :param n_tx: 
    :param expected:
    :return: 
    """
    tc_s, tc_r = generate_tc_pair(n_cp, n_tx)

    # initially everything should have unkonwn state
    is_unknowns = map(lambda tx: tx.validity == ValidityState.Unknown, tc_s.get_unknown_txs())
    assert len(is_unknowns) == n_cp * n_tx
    assert all(is_unknowns)

    # genesis block should be in consensus in round 1
    assert tc_s.consensus_round_of_cp(tc_s.my_chain.chain[0]) == 1
    assert tc_r.consensus_round_of_cp(tc_r.my_chain.chain[0]) == 1

    # final cp block should *not* be in consensus
    assert tc_s.consensus_round_of_cp(tc_s.my_chain.chain[-1]) == -1
    assert tc_r.consensus_round_of_cp(tc_r.my_chain.chain[-1]) == -1

    # second to last cp block should be in consensus
    assert tc_s.consensus_round_of_cp(tc_s.my_chain.chain[-2 - n_tx]) == n_cp
    assert tc_r.consensus_round_of_cp(tc_r.my_chain.chain[-2 - n_tx]) == n_cp

    seq_r = tc_s.my_chain.chain[seq].inner.seq
    resp = tc_r.agreed_pieces(seq_r)

    assert len(tc_s.load_cache_for_verification(seq)) == 0
    assert tc_s.verify_tx(seq, resp) == expected

    if expected == ValidityState.Valid:
        assert len(tc_s.get_unknown_txs()) == len(is_unknowns) - 1
        assert tc_r.vk in tc_s._other_chains
        assert set(tc_s._other_chains[tc_r.vk]).issuperset(resp)

        assert tc_s.load_cache_for_verification(seq) == resp

