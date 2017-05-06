from twisted.internet import task
from base64 import b64encode
from typing import Union
import random
import logging
import time
from collections import defaultdict

from trustchain import TrustChain, TxBlock, CpBlock, Signature, Cons, ValidityState
from src.utils.utils import collate_cp_blocks, my_err_back, encode_n
from src.utils.messages import TxReq, TxResp, SigMsg, SigListMsg, CpMsg, ConsMsg, ValidationReq, \
    ValidationResp, ConsPollMsg


class RoundState:
    def __init__(self):
        self.received_cons = None
        self.received_sigs = {}
        self.received_cps = []
        self.start_time = int(time.time())

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
        if len(self.received_cps) > 0:
            assert self.received_cps[0].round == cp.round
        self.received_cps.append(cp)


class TrustChainRunner:
    """
    We keep a queue of messages and handle them in order
    the handle function by itself essentially pushes the messages into the queue
    """

    def __init__(self, factory, msg_wrapper_f=lambda x: x):
        self.tc = TrustChain()
        self.factory = factory
        self.msg_wrapper_f = msg_wrapper_f
        self.consensus_delay = factory.config.consensus_delay

        self.collect_rubbish_lc = task.LoopingCall(self._collect_rubbish)
        self.collect_rubbish_lc.start(20, False).addErrback(my_err_back)

        self.log_tx_count_lc = task.LoopingCall(self._log_info)
        self.log_tx_count_lc.start(20, False).addErrback(my_err_back)

        self.poll_promoter_lc = task.LoopingCall(self._poll_promoter)
        self.poll_promoter_lc.start(self.factory.config.consensus_delay, False).addErrback(my_err_back)

        self.bootstrap_lc = None
        self.new_consensus_lc = None
        self.new_consensus_lc_count = 0

        self.random_node_for_tx = False
        self.validation_enabled = False

        # attributes below are states for building new CP blocks
        self.round_states = defaultdict(RoundState)

        random.seed()

    def _poll_promoter(self):
        if len(self.factory.promoters) == 0:
            return
        if self.tc.vk in self.factory.promoters:
            return
        # TODO if node misses a round and then promoters go offline, it'll get stuck
        node = random.choice(self.factory.promoters)
        self.send(node, ConsPollMsg(self.tc.latest_round + 1))

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
        return self.tc.consensus_at(r).get_promoters(self.factory.config.n)

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
            assert isinstance(bs.values()[0][0], CpBlock)

            logging.debug("TC: adding cons")
            cons = Cons(r, collate_cp_blocks(bs))
            self.round_states[r].new_cons(cons)

            future_promoters = cons.get_promoters(self.factory.config.n)
            s = Signature(self.tc.vk, self.tc._sk, cons.hash)

            # self.factory.gossip_except(future_promoters, ConsMsg(cons))
            self.factory.multicast(future_promoters, ConsMsg(cons))

            # self.factory.gossip_except(future_promoters + self.factory.promoters, SigMsg(s, r))
            self.factory.multicast(future_promoters + self.factory.promoters, SigMsg(s, r))

            # we also try to add the CP here because we may receive the signatures before the actual CP
            self._try_add_cp(r)

        else:
            logging.debug("TC: not a dict type in handle_cons_from_acs")

    def handle_sigs(self, msg, remote_vk):
        # type: (SigListMsg, str) -> None
        assert isinstance(msg, SigListMsg)
        for s in msg.ss:
            self.handle_sig(SigMsg(s, msg.r), remote_vk)

    def handle_sig(self, msg, remote_vk):
        # type: (SigMsg, str) -> None
        """
        Update round_states on new signature,
        then conditionally gossip
        :param msg: 
        :param remote_vk: 
        :return: 
        """
        assert isinstance(msg, SigMsg)

        logging.debug("TC: received SigMsg {} from {}".format(msg, b64encode(remote_vk)))
        if msg.r >= self.tc.latest_round:
            is_new = self.round_states[msg.r].new_sig(msg.s)
            if is_new:
                self._try_add_cp(msg.r)
                # self.factory.gossip(msg)

    def handle_cp(self, msg, remote_vk):
        # type: (CpMsg, str) -> None
        """
        When I'm the promoter, I expect other nodes to send CPs to me.
        This function handles this situation.
        :param msg: 
        :param remote_vk: 
        :return: 
        """
        assert isinstance(msg, CpMsg)

        logging.debug("TC: received CpMsg {} from {}".format(msg, b64encode(remote_vk)))
        if msg.r >= self.tc.latest_round:
            assert msg.cp.s.vk == remote_vk
            cp = msg.cp
            self.round_states[cp.round].new_cp(cp)

    def handle_cons(self, msg, remote_vk):
        # type: (ConsMsg, str) -> None
        """
        Update round_state on new ConsMsg, 
        then conditionally gossip.
        :param msg: 
        :param remote_vk: 
        :return: 
        """
        assert isinstance(msg, ConsMsg)

        logging.debug("TC: received ConsMsg {} from {}".format(msg, b64encode(remote_vk)))
        if msg.r >= self.tc.latest_round:
            is_new = self.round_states[msg.r].new_cons(msg.cons)
            if is_new:
                self._try_add_cp(msg.r)
                # self.factory.gossip(msg)

    def _try_add_cp(self, r):
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
        if self.round_states[r].received_cons is None:
            logging.debug("TC: don't have consensus result")
            return
        if not self._sufficient_sigs(r):
            logging.debug("TC: insufficient signatures")
            return

        self._add_cp(r)

    def _add_cp(self, r):
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
                       self.factory.promoters)
        if not self.tc.compact_cp_in_consensus(_prev_cp, self.tc.latest_round):
            logging.info("TC: my previous CP not in consensus")

        # new promoters are selected using the latest CP, these promoters are responsible for round r+1
        # no need to continue the ACS for earlier rounds
        assert r == self.tc.latest_round, "{} != {}" \
            .format(r, self.tc.latest_round)
        self.factory.promoters = self._latest_promoters()
        self.factory.acs.stop(self.tc.latest_round)

        assert len(self.factory.promoters) == self.factory.config.n, "{} != {}" \
            .format(len(self.factory.promoters), self.factory.config.n)
        logging.info('TC: CP count in Cons is {}, time taken {}'
                     .format(self.tc.consensus_at(r).count, int(time.time()) - self.round_states[r].start_time))
        logging.info('TC: updated new promoters in round {} to [{}]'
                     .format(r, ",".join(['"' + b64encode(p) + '"' for p in self.factory.promoters])))

        # at this point the promoters are updated
        # finally collect new CP if I'm the promoter, otherwise send CP to promoter
        if self.tc.vk in self.factory.promoters:
            logging.info("TC: I'm a promoter, starting a new consensus round when we have enough CPs")
            self.round_states[r].new_cp(self.tc.my_chain.latest_cp)

            def try_start_acs(_msg, _r):
                self.new_consensus_lc_count += 1
                if self.tc.latest_round >= _r:
                    logging.debug("TC: somebody completed ACS before me, not starting")
                    # setting the following causes the old messages to be dropped
                    self.factory.acs.stop(self.tc.latest_round)
                    self.new_consensus_lc.stop()
                    self.new_consensus_lc_count = 0
                elif len(_msg) < self.factory.config.n and self.new_consensus_lc_count < 10:
                    # we don't have enough CPs to start the consensus, so wait for more until some timeout
                    pass
                else:
                    self.factory.acs.reset_then_start(_msg, _r)
                    self.new_consensus_lc.stop()
                    self.new_consensus_lc_count = 0

            assert self.new_consensus_lc_count == 0, "Overlapping ACS"
            self.new_consensus_lc = task.LoopingCall(try_start_acs, self.round_states[r].received_cps, r + 1)
            self.new_consensus_lc.start(self.consensus_delay, False).addErrback(my_err_back)
        else:
            logging.info("TC: I'm NOT a promoter")

        # send new CP to either all promoters
        # TODO having this if statement for test isn't ideal
        if self.factory.config.test == 'bootstrap':
            self.factory.promoter_cast(CpMsg(self.tc.my_chain.latest_cp))
        else:
            self.factory.promoter_cast_t(CpMsg(self.tc.my_chain.latest_cp))

    def _send_validation_req(self, seq):
        # type: (int) -> None
        """
        Call this function when I want to initiate a instance of the validation protocol.
        First we check the cache and try to validate, if there's nothing in cache send the request.
        :param seq: The sequence number on my side for the TX that I want to validate
        :return: 
        """
        blocks_cache = self.tc.load_cache_for_verification(seq)
        if len(blocks_cache) != 0:
            res = self.tc.verify_tx(seq, blocks_cache)
            if res == ValidityState.Valid:
                logging.info("TC: verified (from cache) {}".format(encode_n(self.tc.my_chain.chain[seq].hash)))
            return

        block = self.tc.my_chain.chain[seq]
        assert isinstance(block, TxBlock)

        if self.factory.config.ignore_promoter and block.inner.counterparty in self.factory.promoters:
            return

        block.request_sent_r = self.tc.latest_round

        assert block.other_half is not None
        seq_r = block.other_half.inner.seq
        node = block.inner.counterparty

        req = ValidationReq(seq, seq_r)
        logging.debug("TC: sent validation to {}, {}".format(b64encode(node), req))
        self.send(node, req)

    def _handle_validation_req(self, req, remote_vk):
        # type: (ValidationReq, str) -> None
        assert isinstance(req, ValidationReq)
        logging.debug("TC: received validation req from {}, {}".format(b64encode(remote_vk), req))

        pieces = self.tc.agreed_pieces(req.seq_r)

        if len(pieces) == 0:
            logging.warning("TC: no pieces, {}".format(sorted(self.tc.consensus_keys)))
            return

        assert len(pieces) > 2

        self.send(remote_vk, ValidationResp(req.seq, req.seq_r, pieces))

    def _handle_validation_resp(self, resp, remote_vk):
        # type: (ValidationResp, str) -> None
        assert isinstance(resp, ValidationResp)
        logging.debug("TC: received validation resp from {}, {}".format(b64encode(remote_vk), resp))

        res = self.tc.verify_tx(resp.seq, resp.pieces)
        if res == ValidityState.Valid:
            logging.info("TC: verified {}".format(encode_n(self.tc.my_chain.chain[resp.seq].hash)))

    def handle(self, msg, src):
        # type: (Union[TxReq, TxResp]) -> None
        """
        Handle messages that are sent using self.send, primarily transaction messages.
        :param msg: 
        :param src: 
        :return: 
        """
        logging.debug("TC: got message".format(msg))
        if isinstance(msg, TxReq):
            nonce = msg.tx.inner.nonce
            m = msg.tx.inner.m
            assert src == msg.tx.sig.vk, "{} != {}".format(b64encode(src), b64encode(msg.tx.sig.vk))
            self.tc.new_tx(src, m, nonce)

            tx = self.tc.my_chain.chain[-1]
            tx.add_other_half(msg.tx)
            self.send(src, TxResp(msg.tx.inner.seq, tx))
            logging.info("TC: added tx (received) {}, from {}".format(encode_n(msg.tx.hash), encode_n(src)))

        elif isinstance(msg, TxResp):
            assert src == msg.tx.sig.vk, "{} != {}".format(b64encode(src), b64encode(msg.tx.sig.vk))
            # TODO index access not safe
            tx = self.tc.my_chain.chain[msg.seq]
            tx.add_other_half(msg.tx)
            logging.info("TC: other half {}".format(encode_n(msg.tx.hash), msg.seq))

        elif isinstance(msg, ValidationReq):
            self._handle_validation_req(msg, src)

        elif isinstance(msg, ValidationResp):
            self._handle_validation_resp(msg, src)

        elif isinstance(msg, ConsPollMsg):
            # TODO handle spam
            if msg.r in self.tc.consensus_keys:
                self.factory.send(src, ConsMsg(self.tc.consensus_at(msg.r)))
                self.factory.send(src, SigListMsg(self.tc.signatures_at(msg.r), msg.r))

        else:
            raise AssertionError("Incorrect message type")

    def send(self, node, msg):
        logging.debug("TC: sending {} to {}".format(self.msg_wrapper_f(msg), b64encode(node)))
        self.factory.send(node, self.msg_wrapper_f(msg))

    def make_tx(self, interval, random_node=False):
        if random_node:
            lc = task.LoopingCall(self._make_tx_rand)
        else:
            node = self.factory.neighbour_if_even()
            if node is None:
                # we do nothing, since we're not an even index
                return
            assert node != self.factory.vk

            lc = task.LoopingCall(self._make_tx, node)

        lc.start(interval).addErrback(my_err_back)

    def _make_tx_rand(self):
        if self.factory.config.ignore_promoter:
            node = self.factory.random_non_promoter
            if node is None:
                return
        else:
            node = self.factory.random_node
        self._make_tx(node)

    def _make_tx(self, node):
        """
        only use this in LoopingCall, not continuous transaction
        :param node: 
        :return: 
        """
        if self.factory.config.ignore_promoter and self.tc.vk in self.factory.promoters:
            return

        # throttle transactions if we cannot validate them timely
        if self.validation_enabled and len(self.tc.get_unknown_txs()) > 20 * self.factory.config.n:
            logging.info("TC: throttling")
            return

        # cannot be myself
        assert node != self.factory.vk

        # typical bitcoin tx is 500 bytes
        m = 'a' * random.randint(400, 600)
        logging.debug("TC: {} making tx to".format(encode_n(node)))

        # create the tx and send the request
        self.tc.new_tx(node, m)
        tx = self.tc.my_chain.chain[-1]
        self.send(node, TxReq(tx))
        logging.info("TC: added tx {}, from {}".format(encode_n(tx.hash), encode_n(self.tc.vk)))

    def make_validation(self, interval):
        lc = task.LoopingCall(self._validate_random_tx)
        lc.start(interval).addErrback(my_err_back)
        self.validation_enabled = True

    def _validate_random_tx(self):
        """
        Each call sends validation requests for all unvalidated TX
        :return: 
        """
        if self.factory.config.ignore_promoter and self.tc.vk in self.factory.promoters:
            return

        if self.tc.latest_cp.round < 2:
            return

        max_h = self.tc.my_chain.get_cp_of_round(self.tc.latest_cp.round - 1).seq
        txs = filter(lambda _tx: _tx.seq < max_h and _tx.request_sent_r < self.tc.latest_round,
                     self.tc.get_unknown_txs())

        if len(txs) == 0:
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
        self.factory.promoter_cast(CpMsg(self.tc.genesis))

        def bootstrap_when_ready():
            if self.factory.vk in self.factory.promoters:
                logging.info("TC: bootstrap_lc, got {} CPs".format(len(self.round_states[0].received_cps)))
                # collect CPs of round 0, from it, create consensus result of round 1
                if len(self.round_states[0].received_cps) >= n:
                    cps = self.round_states[0].received_cps
                    self.factory.acs.start(cps, 1)
                    self.bootstrap_lc.stop()
            else:
                logging.info(
                    "TC: bootstrap_lc, not promoter, got {} CPs".format(len(self.round_states[0].received_cps)))
                self.bootstrap_lc.stop()

        self.bootstrap_lc = task.LoopingCall(bootstrap_when_ready)
        self.bootstrap_lc.start(5, False).addErrback(my_err_back)
