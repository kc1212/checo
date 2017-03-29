from base64 import b64encode
from typing import Dict, Union
import logging

from .bracha import Bracha
from .mo14 import Mo14
from src.utils.messages import ACSMsg, BrachaMsg, Mo14Msg
from src.utils.utils import Replay, Handled, dictionary_hash


class ACS:
    def __init__(self, factory):
        self.factory = factory
        self.round = -1  # type: int
        self.done = False
        # the following are initialised at start
        self.brachas = {}  # type: Dict[str, Bracha]
        self.mo14s = {}  # type: Dict[str, Mo14]
        self.bracha_results = {}  # type: Dict[str, str]
        self.mo14_results = {}  # type: Dict[str, int]
        self.mo14_provided = {}  # type: Dict[str, int]

    def reset(self):
        """
        :return:
        """
        logging.debug("ACS: resetting...")
        self.done = False
        self.brachas = {}  # type: Dict[str, Bracha]
        self.mo14s = {}  # type: Dict[str, Mo14]
        self.bracha_results = {}  # type: Dict[str, str]
        self.mo14_results = {}  # type: Dict[str, int]
        self.mo14_provided = {}  # type: Dict[str, int]

    def stop(self, r):
        """
        Calling this will ignore messages on or before round r
        :param r: 
        :return: 
        """
        logging.debug("ACS: stopping...")
        self.reset()
        self.round = r
        self.done = True

    def start(self, msg, r):
        """
        initialise our RBC and BA instances
        assume all the promoters are connected
        :param msg: the message to propose
        :param r: the consensus round
        :return:
        """
        assert len(self.factory.promoters) == self.factory.config.n
        self.round = r

        for promoter in self.factory.peers.keys():
            logging.debug("ACS: adding promoter {}".format(b64encode(promoter)))

            def msg_wrapper_f_factory(instance, round):
                def f(_msg):
                    return ACSMsg(instance, round, _msg)
                return f

            self.brachas[promoter] = Bracha(self.factory, msg_wrapper_f_factory(promoter, self.round))
            self.mo14s[promoter] = Mo14(self.factory, msg_wrapper_f_factory(promoter, self.round))

        my_vk = self.factory.vk
        assert my_vk in self.brachas
        assert my_vk in self.mo14s

        # send the first RBC, assume all nodes have connected
        logging.info("ACS: initiating {} with {}".format(b64encode(my_vk), msg))
        self.brachas[my_vk].bcast_init(msg)

    def reset_then_start(self, msg, r):
        self.reset()
        self.start(msg, r)

    def handle(self, msg, sender_vk):
        # type: (ACSMsg, str) -> Union[Handled, Replay]
        """
        Msg {
            instance: String // vk
            ty: u32
            round: u32 // this is not the same as the Mo14 'r'
            body: Bracha | Mo14 // defined by ty
        }
        :param msg: acs header with vk followed by either a 'bracha' message or a 'mo14' message
        :param sender_vk: the vk of the sender
        :return: the agreed subset on completion otherwise None
        """
        logging.debug("ACS: got msg (instance: {}, round: {}) from {}".format(b64encode(msg.instance),
                                                                              msg.round, b64encode(sender_vk)))

        if msg.round < self.round:
            logging.debug("ACS: round already over, curr: {}, required: {}".format(self.round, msg.round))
            return Handled()

        if msg.round > self.round:
            logging.debug("ACS: round is not ready, curr: {}, required: {}".format(self.round, msg.round))
            return Replay()

        if self.done:
            logging.debug("ACS: we're done, doing nothing")
            return Handled()

        instance = msg.instance
        round = msg.round
        assert round == self.round
        body = msg.body
        t = self.factory.config.t
        n = self.factory.config.n

        if isinstance(body, BrachaMsg):
            if instance not in self.brachas:
                logging.debug("instance {} not in self.brachas".format(b64encode(instance)))
                return Replay()
            res = self.brachas[instance].handle(body)
            if isinstance(res, Handled) and res.m is not None:
                logging.debug("ACS: Bracha delivered for {}, {}".format(b64encode(instance), res.m))
                self.bracha_results[instance] = res.m
                if instance not in self.mo14_provided:
                    logging.debug("ACS: initiating BA for {}, {}".format(b64encode(instance), 1))
                    self.mo14_provided[instance] = 1
                    self.mo14s[instance].start(1)

        elif isinstance(body, Mo14Msg):
            if instance in self.mo14_provided:
                logging.debug("ACS: forwarding Mo14")
                res = self.mo14s[instance].handle(body, sender_vk)
                if isinstance(res, Handled) and res.m is not None:
                    logging.debug("ACS: delivered Mo14 for {}, {}".format(b64encode(instance), res.m))
                    self.mo14_results[instance] = res.m
                elif isinstance(res, Replay):
                    # raise AssertionError("Impossible, our Mo14 instance already instantiated")
                    return Replay()

            ones = [v for k, v in self.mo14_results.iteritems() if v == 1]
            if len(ones) >= n - t:
                difference = set(self.mo14s.keys()) - set(self.mo14_provided.keys())
                logging.debug("ACS: got n - t 1s")
                logging.debug("difference = {}".format(difference))
                for d in list(difference):
                    logging.debug("ACS: initiating BA for {}, v".format(b64encode(d), 0))
                    self.mo14_provided[d] = 0
                    self.mo14s[d].start(0)

            if instance not in self.mo14_provided:
                logging.debug("ACS: got BA before RBC...")
                # if we got a BA instance, but we haven't deliver its corresponding RBC,
                # we instruct the caller to replay the message
                return Replay()

        else:
            raise AssertionError("ACS: invalid payload type")

        if len(self.mo14_results) >= n:
            # return the result if we're done, otherwise return None
            assert n == len(self.mo14_results)

            self.done = True
            res = self.collate_results()
            # NOTE we just print the hash of the results and compare, the actual output is too much...
            logging.info("ACS: DONE \"{}\"".format(b64encode(dictionary_hash(res[0]))))
            return Handled(res)
        return Handled()

    def collate_results(self):
        key_of_ones = [k for k, v in self.mo14_results.iteritems() if v == 1]
        res = {k: self.bracha_results[k] for k in key_of_ones}
        return res, self.round
