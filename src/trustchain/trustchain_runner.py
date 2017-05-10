import logging
import random
import time
from base64 import b64encode
from collections import defaultdict

from twisted.internet import task
from typing import Union

from src.messages.messages import TxReq, TxResp, SigMsg, CpMsg, ConsMsg, ValidationReq, \
    ValidationResp, AskConsMsg
from src.trustchain.trustchain import TrustChain, TxBlock, CpBlock, Signature, Cons
from src.utils.utils import collate_cp_blocks, my_err_back, encode_n, call_later


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

    def __init__(self, factory, msg_wrapper_f=lambda x: x):
        self.tc = TrustChain()
        self.factory = factory
        self.msg_wrapper_f = msg_wrapper_f
        self.consensus_delay = factory.config.consensus_delay

        self.collect_rubbish_lc = task.LoopingCall(self._collect_rubbish)
        self.collect_rubbish_lc.start(self.consensus_delay, False).addErrback(my_err_back)

        self.log_tx_count_lc = task.LoopingCall(self._log_info)
        self.log_tx_count_lc.start(20, False).addErrback(my_err_back)

        self.bootstrap_lc = None

        self.random_node_for_tx = False
        self.validation_enabled = False

        # attributes below are states for building new CP blocks
        self.round_states = defaultdict(RoundState)

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
            assert isinstance(bs.values()[0][0], CpBlock)

            logging.debug("TC: adding cons")
            cons = Cons(r, collate_cp_blocks(bs))
            self.round_states[r].new_cons(cons)

            future_promoters = cons.get_promoters(self.factory.config.n)
            s = Signature.new(self.tc.vk, self.tc._sk, cons.hash)

            self.factory.gossip_except(future_promoters, ConsMsg(cons))
            self.factory.multicast(future_promoters, ConsMsg(cons))

            sig_set = list(set(future_promoters) | set(self.factory.promoters))
            self.factory.gossip_except(sig_set, SigMsg(s, r))
            self.factory.multicast(sig_set, SigMsg(s, r))

            # we also try to add the CP here because we may receive the signatures before the actual CP
            self._try_add_cp(r)

        else:
            logging.debug("TC: not a dict type in handle_cons_from_acs")

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
                self.factory.gossip(msg)

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
                self.factory.gossip(msg)

    def handle_ask_cons(self, msg, remote_vk):
        assert isinstance(msg, AskConsMsg)
        # TODO vulnerable to spam
        if msg.r in self.tc.consensus:
            # NOTE: we use factory.send because ConsMsg is handled separately
            self.factory.send(remote_vk, ConsMsg(self.tc.consensus[msg.r]))

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
        if not self._sufficient_sigs(r):
            logging.debug("TC: insufficient signatures")
            return
        if self.round_states[r].received_cons is None:
            # if we're here, it means we have enough signatures but still no consensus result
            # manually ask for it from the promoters only once, ideally this should be dynamic
            if not self.round_states[r].asked:
                logging.info("TC: round {}, don't have consensus result, asking...".format(r))
                self.factory.send(random.choice(self.factory.promoters), AskConsMsg(r))
                self.round_states[r].asked = True
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
            logging.info("TC: round {}, my previous CP not in consensus".format(r))

        # new promoters are selected using the latest CP, these promoters are responsible for round r+1
        # no need to continue the ACS for earlier rounds
        assert r == self.tc.latest_round, "{} != {}" \
            .format(r, self.tc.latest_round)
        self.factory.promoters = self._latest_promoters()
        self.factory.acs.stop(self.tc.latest_round)

        assert len(self.factory.promoters) == self.factory.config.n, "{} != {}" \
            .format(len(self.factory.promoters), self.factory.config.n)
        logging.info('TC: round {}, CP count in Cons is {}, time taken {}'
                     .format(r, self.tc.consensus[r].count, int(time.time()) - self.round_states[r].start_time))
        logging.info('TC: round {}, updated new promoters to [{}]'
                     .format(r, ",".join(['"' + b64encode(p) + '"' for p in self.factory.promoters])))

        # at this point the promoters are updated
        # finally collect new CP if I'm the promoter, otherwise send CP to promoter
        if self.tc.vk in self.factory.promoters:
            logging.info("TC: round {}, I'm a promoter, starting a new consensus round when we have enough CPs"
                         .format(r))
            self.round_states[r].new_cp(self.tc.my_chain.latest_cp)

            def _try_start_acs(_r):
                # NOTE: here we assume the consensus should have a length >= n
                _msg = self.round_states[r].received_cps
                if self.tc.latest_round >= _r:
                    logging.info("TC: round {}, somebody completed ACS before me, not starting".format(_r))
                    # setting the following causes the old messages to be dropped
                    self.factory.acs.stop(self.tc.latest_round)
                else:
                    logging.info("TC: round {}, starting ACS with {} CPs".format(_r, len(_msg)))
                    self.factory.acs.reset_then_start(_msg, _r)

            call_later(self.consensus_delay, _try_start_acs, r + 1)
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

        if not pieces:
            logging.warning("TC: no pieces, {}".format(sorted(self.tc.consensus.keys())))
            return

        assert len(pieces) > 2

        self.send(remote_vk, ValidationResp(req.seq, req.seq_r, pieces))

    def _handle_validation_resp(self, resp, remote_vk):
        # type: (ValidationResp, str) -> None
        assert isinstance(resp, ValidationResp)
        logging.debug("TC: received validation resp from {}, {}".format(b64encode(remote_vk), resp))

        self.tc.verify_tx(resp.seq, resp.pieces)

    def handle(self, msg, remote_vk):
        # type: (Union[TxReq, TxResp]) -> None
        """
        Handle messages that are sent using self.send, primarily transaction messages.
        :param msg: 
        :param remote_vk: 
        :return: 
        """
        logging.debug("TC: got message {}".format(msg))
        if isinstance(msg, TxReq):
            nonce = msg.tx.inner.nonce
            m = msg.tx.inner.m
            assert remote_vk == msg.tx.s.vk, "{} != {}".format(b64encode(remote_vk), b64encode(msg.tx.s.vk))
            self.tc.new_tx(remote_vk, m, nonce)

            # tx cannot be a CpBlock because we just called new_tx
            tx = self.tc.my_chain.chain[-1]
            tx.add_other_half(msg.tx)
            self.send(remote_vk, TxResp(msg.tx.inner.seq, tx))
            logging.info("TC: added tx (received) {}, from {}".format(encode_n(msg.tx.hash), encode_n(remote_vk)))

        elif isinstance(msg, TxResp):
            assert remote_vk == msg.tx.s.vk, "{} != {}".format(b64encode(remote_vk), b64encode(msg.tx.s.vk))
            # TODO index access not safe
            tx = self.tc.my_chain.chain[msg.seq]
            tx.add_other_half(msg.tx)
            logging.info("TC: other half {}".format(encode_n(msg.tx.hash)))

        elif isinstance(msg, ValidationReq):
            self._handle_validation_req(msg, remote_vk)

        elif isinstance(msg, ValidationResp):
            self._handle_validation_resp(msg, remote_vk)

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
        if self.validation_enabled and len(self.tc.get_verifiable_txs()) > 20 * self.factory.config.n:
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

        txs = self.tc.get_verifiable_txs()

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
