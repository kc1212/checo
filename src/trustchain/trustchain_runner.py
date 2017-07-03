import logging
import random
import time
from base64 import b64encode
from collections import defaultdict

from twisted.internet import task

import src.messages.messages_pb2 as pb
from src.trustchain.trustchain import TrustChain, TxBlock, CpBlock, Signature, Cons, CompactBlock
from src.utils import collate_cp_blocks, my_err_back, encode_n


class RoundState(object):
    def __init__(self):
        self.received_cons = None
        self.received_sigs = {}
        self.received_cps = []
        self.start_time = int(time.time())
        self.asked = False

    def __str__(self):
        return "received cons: {}, sig count: {}, cp count: {}"\
            .format("yes" if self.received_cons is not None else "no", len(self.received_sigs), len(self.received_cps))

    def new_cons(self, cons):
        # type: (Cons) -> bool
        assert isinstance(cons, Cons)
        if self.received_cons is None:
            self.received_cons = cons
            return True

        # TODO eventually we need to store all received cons and check which are correctly and sufficiently signed
        assert cons == self.received_cons
        return False

    def new_sig(self, s):
        # type: (Signature) -> bool
        """
        :param s: 
        :return: True if it is new, otherwise False
        """
        assert isinstance(s, Signature)
        if s.vk not in self.received_sigs:
            self.received_sigs[s.vk] = s
            return True

        # TODO we should handle this
        assert self.received_sigs[s.vk] == s
        return False

    def new_cp(self, cp):
        # type: (CpBlock) -> None
        assert isinstance(cp, CpBlock)
        if self.received_cps:
            assert self.received_cps[0].round == cp.round
        self.received_cps.append(cp)


class TrustChainRunner(object):
    """
    We keep a queue of messages and handle them in order
    the handle function by itself essentially pushes the messages into the queue
    """

    def __init__(self, factory):
        self.tc = TrustChain()
        self.factory = factory

        self.collect_rubbish_lc = task.LoopingCall(self._collect_rubbish)
        self.collect_rubbish_lc.start(5, False).addErrback(my_err_back)

        self.log_tx_count_lc = task.LoopingCall(self._log_info)
        self.log_tx_count_lc.start(5, False).addErrback(my_err_back)

        self.bootstrap_lc = None

        self.random_node_for_tx = False

        # attributes below are states for building new CP blocks
        self.round_states = defaultdict(RoundState)

        self._initial_promoters = []

        random.seed()

    def _log_info(self):
        logging.info("TC: current tx count {}, validated {}".format(self.tc.tx_count, len(self.tc.get_validated_txs())))

    def _sufficient_sigs(self, r):
        if len(self.round_states[r].received_sigs) > self.factory.config.t:
            return True
        return False

    def _collect_rubbish(self):
        for k in self.round_states.keys():
            if k < self.tc.latest_round:
                logging.debug("TC: pruning key {}".format(k))
                del self.round_states[k]
        # logging.info("TC: states - {}".format(self.round_states))

    def _latest_promoters(self):
        r = self.tc.latest_round
        return self._promoter_of_round(r)

    def _promoter_of_round(self, r):
        if r == 0:
            return self._initial_promoters
        return self.tc.consensus[r].get_promoters(self.factory.config.n)

    def handle_cons_from_acs(self, msg):
        """
        This is only called after we get the output from ACS
        :param msg:
        :return:
        """
        bs, r = msg
        logging.debug("TC: handling cons from ACS {}, round {}".format(bs, r))

        if isinstance(bs, dict):
            assert len(bs) > 0

            def _parse_cps(_b):
                _cps = pb.CpBlocks()
                _cps.ParseFromString(_b)
                return [CpBlock(_cp) for _cp in _cps.cps]

            cps = {k: _parse_cps(v) for k, v in bs.iteritems()}
            cons = Cons.new(r, [cp.pb for cp in collate_cp_blocks(cps)])
            self.round_states[r].new_cons(cons)

            s = Signature.new(self.tc.vk, self.tc._sk, cons.hash)

            self.factory.bcast(cons.pb)
            self.factory.bcast(pb.SigWithRound(s=s.pb, r=r))

            # we also try to add the CP here because we may receive the signatures before the actual CP
            self._try_add_cp(r)

        else:
            logging.debug("TC: not a dict type in handle_cons_from_acs")

    def handle_sig(self, msg, remote_vk):
        # type: (pb.SigWithRound, str) -> None
        """
        Update round_states on new signature,
        then conditionally gossip
        :param msg: 
        :param remote_vk: 
        :return: 
        """
        assert isinstance(msg, pb.SigWithRound)
        logging.debug("TC: received SigWithRound {} from {}".format(msg, b64encode(remote_vk)))

        sig = Signature(msg.s)

        if msg.r >= self.tc.latest_round:
            is_new = self.round_states[msg.r].new_sig(sig)
            if is_new:
                self._try_add_cp(msg.r)

    def handle_cp(self, msg, remote_vk):
        # type: (pb.CpBlock, str) -> None
        """
        When I'm the promoter, I expect other nodes to send CPs to me.
        This function handles this situation.
        :param msg: 
        :param remote_vk: 
        :return: 
        """
        assert isinstance(msg, pb.CpBlock)
        logging.debug("TC: received CpBlock {} from {}".format(msg, b64encode(remote_vk)))

        cp = CpBlock(msg)

        if cp.round >= self.tc.latest_round:
            assert cp.s.vk == remote_vk
            self.round_states[cp.round].new_cp(cp)

    def handle_cons(self, msg, remote_vk):
        # type: (pb.Cons, str) -> None
        """
        Update round_state on new consensus message, 
        then conditionally gossip.
        :param msg: 
        :param remote_vk: 
        :return: 
        """
        assert isinstance(msg, pb.Cons)
        logging.debug("TC: received Cons {} from {}".format(msg, b64encode(remote_vk)))

        cons = Cons(msg)

        if cons.round >= self.tc.latest_round:
            is_new = self.round_states[cons.round].new_cons(cons)
            if is_new:
                self._try_add_cp(cons.round)

    def handle_ask_cons(self, msg, remote_vk):
        # type: (pb.AskCons, str) -> None
        """
        If we have the consensus result, send it to the requester.
        TODO vulnerable to spam
        :param msg: 
        :param remote_vk: 
        :return: 
        """
        assert isinstance(msg, pb.AskCons)
        if msg.r in self.tc.consensus:
            self.send(remote_vk, self.tc.consensus[msg.r].pb)

    def _try_add_cp(self, r):
        # type: (int) -> None
        """
        Try to add my own CP from the received consensus results and signatures
        The input parameter is a bit strange, we don't add the cp from the parameter, but from the buffer round_states
        we don't need lock here because this function runs atomically
        :param r: 
        :return: 
        """
        if self.tc.latest_round >= r:
            logging.debug("TC: already added the CP")
            return
        if not self._sufficient_sigs(r):
            logging.debug("TC: insufficient signatures")
            return
        if self.round_states[r].received_cons is None:
            # if we're here, it means we have enough signatures but still no consensus result
            # manually ask for it from the promoters only once, ideally this should be dynamic

            # NOTE not necessary anymore because promoters now broadcast
            # if not self.round_states[r].asked:
            #     logging.info("TC: round {}, don't have consensus result, asking...".format(r))
            #     self.send(random.choice(self.factory.promoters), pb.AskCons(r=r))
            #     self.round_states[r].asked = True
            return

        try:
            self._promoter_of_round(r - 1)
        except KeyError:
            self.send(random.choice(self.factory.promoters), pb.AskCons(r=r-1))
            return

        self._add_cp(r)

    def _add_cp(self, r):
        # type: (int) -> None
        """
        :param r:
        :return:
        """
        # here we create a new CP from the consensus result (both of round r)
        logging.debug("TC: adding CP in round {}".format(r))
        _prev_cp = self.tc.latest_cp.compact  # this is just for logging
        self.tc.new_cp(1,
                       self.round_states[r].received_cons,
                       self.round_states[r].received_sigs.values(),
                       self._promoter_of_round(r - 1),
                       self.factory.config.t)
        if not self.tc.compact_cp_in_consensus(_prev_cp, self.tc.latest_round):
            logging.info("TC: round {}, my previous CP not in consensus".format(r))

        # new promoters are selected using the latest CP, these promoters are responsible for round r+1
        # no need to continue the ACS for earlier rounds
        assert r == self.tc.latest_round,\
            "{} != {}".format(r, self.tc.latest_round)
        self.factory.promoters = self._latest_promoters()
        self.factory.acs.stop(self.tc.latest_round)

        assert len(self.factory.promoters) == self.factory.config.n,\
            "{} != {}".format(len(self.factory.promoters), self.factory.config.n)
        logging.info('TC: round {}, CP count in Cons is {}, time taken {}'
                     .format(r, self.tc.consensus[r].count, int(time.time()) - self.round_states[r].start_time))
        logging.info('TC: round {}, updated new promoters to [{}]'
                     .format(r, ",".join(['"' + b64encode(p) + '"' for p in self.factory.promoters])))
        self.factory.log_communication_costs("TC: round {},".format(r))

        # at this point the promoters are updated
        # finally collect new CP if I'm the promoter, otherwise send CP to promoter
        if self.tc.vk in self.factory.promoters:

            # do not start ACS if I'm Byzantine
            if self.factory.config.auto_byzantine and \
                            sorted(self.factory.promoters).index(self.tc.vk) < self.factory.config.t:
                logging.info("TC: round {}, I'm a Byzantine promoter".format(r))

            else:
                logging.info("TC: round {}, I'm a promoter, starting a new consensus round when we have enough CPs"
                             .format(r))
                self.round_states[r].new_cp(self.tc.my_chain.latest_cp)

                class LoopingStartACS(object):
                    def __init__(self, _p):
                        # type: (TrustChainRunner) -> None
                        self.p = _p
                        self.lc = None

                    def try_start_acs(self, _r):
                        assert self.lc
                        # NOTE: we take CPs of round r - 1 to create consensus result of round r
                        _msg = [cp.pb for cp in self.p.round_states[_r - 1].received_cps]
                        if self.p.tc.latest_round >= _r:
                            logging.info("TC: round {}, somebody completed ACS before me, not starting".format(_r))
                            # setting the following causes the old messages to be dropped
                            self.p.factory.acs.stop(self.p.tc.latest_round)
                            self.lc.stop()
                            self.lc = None
                        elif len(_msg) >= self.p.factory.config.population - self.p.factory.config.t:
                            logging.info("TC: round {}, starting ACS with {} CPs".format(_r, len(_msg)))
                            self.p.factory.acs.reset_then_start(pb.CpBlocks(cps=_msg).SerializeToString(), _r)
                            self.lc.stop()
                            self.lc = None
                        else:
                            logging.info("TC: round {}, not enough CPs {}".format(_r, len(_msg)))

                lc_acs = LoopingStartACS(self)
                lc = task.LoopingCall(lc_acs.try_start_acs, r + 1)
                lc_acs.lc = lc

                lc.start(2, False).addErrback(my_err_back)

        else:
            logging.info("TC: round {}, I'm NOT a promoter".format(r))

        # send new CP to either all promoters
        self.factory.promoter_cast(self.tc.my_chain.latest_cp.pb)

    def _send_validation_req(self, seq):
        # type: (int) -> None
        """
        Call this function when I want to initiate a instance of the validation protocol.
        :param seq: The sequence number on my side for the TX that I want to validate
        :return: 
        """
        block = self.tc.my_chain.chain[seq]
        assert isinstance(block, TxBlock)

        if self.factory.config.ignore_promoter and block.inner.counterparty in self.factory.promoters:
            return

        block.request_sent_r = self.tc.latest_round

        assert block.other_half is not None
        seq_r = block.other_half.inner.seq
        node = block.inner.counterparty

        req = pb.ValidationReq(seq=seq, seq_r=seq_r)
        logging.debug("TC: sent validation to {}, {}".format(b64encode(node), req))
        self.send(node, req)

    def handle_validation_req(self, req, remote_vk):
        # type: (pb.ValidationReq, str) -> None
        assert isinstance(req, pb.ValidationReq)
        logging.debug("TC: received validation req from {}, {}".format(b64encode(remote_vk), req))

        pieces = self.tc.agreed_pieces(req.seq_r)

        if not pieces:
            logging.warning("TC: no pieces, {}".format(sorted(self.tc.consensus.keys())))
            return

        assert len(pieces) > 2

        self.send(remote_vk, pb.ValidationResp(seq=req.seq, seq_r=req.seq_r, pieces=[p.pb for p in pieces]))

    def handle_validation_resp(self, resp, remote_vk):
        # type: (pb.ValidationResp, str) -> None
        """
        Try to validate the pieces that we just received.
        Note that tc.verify_tx will also validate additional transactions when there's sufficient information in cache.
        :param resp: 
        :param remote_vk: 
        :return: 
        """
        assert isinstance(resp, pb.ValidationResp)
        logging.debug("TC: received validation resp from {}, {}".format(b64encode(remote_vk), resp))

        self.tc.verify_tx(resp.seq, [CompactBlock(p) for p in resp.pieces])

    def handle_tx_req(self, msg, remote_vk):
        # type: (pb.TxReq, str) -> None
        assert isinstance(msg, pb.TxReq)

        nonce = msg.tx.inner.nonce
        m = msg.tx.inner.m

        assert remote_vk == msg.tx.s.vk, "{} != {}".format(b64encode(remote_vk), b64encode(msg.tx.s.vk))
        self.tc.new_tx(remote_vk, m, nonce)

        # new_tx cannot be a CpBlock because we just called new_tx
        new_tx = self.tc.my_chain.chain[-1]
        new_tx.add_other_half(TxBlock(msg.tx))
        self.send(remote_vk, pb.TxResp(seq=msg.tx.inner.seq, tx=new_tx.pb))
        logging.debug("TC: added tx (received) {}, from {}"
                      .format(encode_n(new_tx.other_half.hash), encode_n(remote_vk)))

    def handle_tx_resp(self, msg, remote_vk):
        # type: (pb.TxResp, str) -> None
        assert isinstance(msg, pb.TxResp)
        assert remote_vk == msg.tx.s.vk, "{} != {}".format(b64encode(remote_vk), b64encode(msg.tx.s.vk))
        # TODO index access not safe
        tx = self.tc.my_chain.chain[msg.seq]
        tx.add_other_half(TxBlock(msg.tx))
        logging.debug("TC: other half {}".format(encode_n(tx.hash)))

    def send(self, node, msg):
        self.factory.send(node, msg)

    def make_tx(self, interval, random_node=False):
        # type: (float, bool) -> None
        """
        Entry point for making transactions periodically.
        :param interval: 
        :param random_node: 
        :return: 
        """
        if random_node:
            lc = task.LoopingCall(lambda: self._make_tx(self.factory.random_node))
        else:
            node = self.factory.neighbour
            lc = task.LoopingCall(self._make_tx, node)

        lc.start(interval).addErrback(my_err_back)

    def _make_tx(self, node):
        if self.factory.config.ignore_promoter:
            if self.tc.vk in self.factory.promoters or node in self.factory.promoters:
                return

        # cannot be myself
        assert node != self.factory.vk

        # typical bitcoin tx is 500 bytes
        m = 'a' * random.randint(400, 600)
        logging.debug("TC: {} making tx to".format(encode_n(node)))

        # create the tx and send the request
        self.tc.new_tx(node, m)
        tx = self.tc.my_chain.chain[-1]
        self.send(node, pb.TxReq(tx=tx.pb))
        logging.debug("TC: added tx {}, from {}".format(encode_n(tx.hash), encode_n(self.tc.vk)))

    def make_validation(self, interval):
        # type: (float) -> None
        """
        Entry point for making validations periodically.
        :param interval: 
        :return: 
        """
        lc = task.LoopingCall(self._validate_random_tx)
        lc.start(interval).addErrback(my_err_back)

    def _validate_random_tx(self):
        """
        Each call sends validation requests for all unvalidated TX
        :return: 
        """
        if self.factory.config.ignore_promoter and self.tc.vk in self.factory.promoters:
            return

        if self.tc.latest_cp.round < 2:
            return

        txs = filter(lambda tx: tx.request_sent_r == -1, self.tc.get_verifiable_txs())

        if not txs:
            return

        self._send_validation_req(random.choice(txs).seq)

    def bootstrap_promoters(self):
        """
        Assume all the nodes are already online, exchange genesis blocks, and start ACS.
        The first n values, sorted by vk, are promoters
        :return:
        """
        n = self.factory.config.n
        self.factory.promoters = sorted(self.factory.peers.keys())[:n]
        self.factory.promoter_cast(self.tc.genesis.pb)

        self._initial_promoters = self.factory.promoters

        def bootstrap_when_ready():
            if self.factory.vk in self.factory.promoters:
                logging.info("TC: bootstrap_lc, got {} CPs".format(len(self.round_states[0].received_cps)))
                # collect CPs of round 0, from it, create consensus result of round 1
                if len(self.round_states[0].received_cps) >= n:
                    msg = pb.CpBlocks(cps=[cp.pb for cp in self.round_states[0].received_cps])
                    self.factory.acs.start(msg.SerializeToString(), 1)
                    self.bootstrap_lc.stop()
            else:
                logging.info(
                    "TC: bootstrap_lc, not promoter, got {} CPs".format(len(self.round_states[0].received_cps)))
                self.bootstrap_lc.stop()

        self.bootstrap_lc = task.LoopingCall(bootstrap_when_ready)
        self.bootstrap_lc.start(5, False).addErrback(my_err_back)
