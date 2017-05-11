import logging
import random
from base64 import b64encode

from typing import Dict, Union

import src.messages.messages_pb2 as pb
from src.utils import Replay, Handled, dictionary_hash
from .bracha import Bracha
from .mo14 import Mo14


class ACS(object):
    def __init__(self, factory):
        self._factory = factory
        self._round = -1  # type: int
        self._done = False
        # the following are initialised at start
        self._brachas = {}  # type: Dict[str, Bracha]
        self._mo14s = {}  # type: Dict[str, Mo14]
        self._bracha_results = {}  # type: Dict[str, str]
        self._mo14_results = {}  # type: Dict[str, int]
        self._mo14_provided = {}  # type: Dict[str, int]

    def reset(self):
        """
        :return:
        """
        logging.debug("ACS: resetting...")
        self._done = False
        self._brachas = {}  # type: Dict[str, Bracha]
        self._mo14s = {}  # type: Dict[str, Mo14]
        self._bracha_results = {}  # type: Dict[str, str]
        self._mo14_results = {}  # type: Dict[str, int]
        self._mo14_provided = {}  # type: Dict[str, int]

    def stop(self, r):
        """
        Calling this will ignore messages on or before round r
        :param r: 
        :return: 
        """
        logging.debug("ACS: stopping...")
        self.reset()
        self._round = r
        self._done = True

    def start(self, msg, r):
        """
        initialise our RBC and BA instances
        assume all the promoters are connected
        :param msg: the message to propose
        :param r: the consensus round
        :return:
        """
        assert len(self._factory.promoters) == self._factory.config.n

        self._round = r

        for promoter in self._factory.promoters:
            logging.debug("ACS: adding promoter {}".format(b64encode(promoter)))

            def msg_wrapper_f_factory(_instance, _round):
                def f(_msg):
                    if isinstance(_msg, pb.Bracha):
                        return pb.ACS(instance=_instance, round=_round, bracha=_msg)
                    elif isinstance(_msg, pb.Mo14):
                        return pb.ACS(instance=_instance, round=_round, mo14=_msg)
                    else:
                        raise AssertionError("Invalid wrapper input")
                return f

            self._brachas[promoter] = Bracha(self._factory, msg_wrapper_f_factory(promoter, self._round))
            self._mo14s[promoter] = Mo14(self._factory, msg_wrapper_f_factory(promoter, self._round))

        my_vk = self._factory.vk
        assert my_vk in self._brachas
        assert my_vk in self._mo14s

        # send the first RBC, assume all nodes have connected
        logging.info("ACS: initiating vk {}, msg {}".format(b64encode(my_vk), b64encode(msg)))
        self._brachas[my_vk].bcast_init(msg)

    def reset_then_start(self, msg, r):
        self.reset()
        self.start(msg, r)

    def handle(self, msg, sender_vk):
        # type: (pb.ACS, str) -> Union[Handled, Replay]
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

        if msg.round < self._round:
            logging.debug("ACS: round already over, curr: {}, required: {}".format(self._round, msg.round))
            return Handled()

        if msg.round > self._round:
            logging.debug("ACS: round is not ready, curr: {}, required: {}".format(self._round, msg.round))
            return Replay()

        if self._done:
            logging.debug("ACS: we're done, doing nothing")
            return Handled()

        instance = msg.instance
        round = msg.round
        assert round == self._round

        t = self._factory.config.t
        n = self._factory.config.n

        body_type = msg.WhichOneof('body')

        if body_type == 'bracha':
            if instance not in self._brachas:
                logging.debug("instance {} not in self.brachas".format(b64encode(instance)))
                return Replay()
            res = self._brachas[instance].handle(msg.bracha, sender_vk)
            if isinstance(res, Handled) and res.m is not None:
                logging.debug("ACS: Bracha delivered for {}, {}".format(b64encode(instance), res.m))
                self._bracha_results[instance] = res.m
                if instance not in self._mo14_provided:
                    logging.debug("ACS: initiating BA for {}, {}".format(b64encode(instance), 1))
                    self._mo14_provided[instance] = 1
                    self._mo14s[instance].start(1)

        elif body_type == 'mo14':
            if instance in self._mo14_provided:
                logging.debug("ACS: forwarding Mo14")
                res = self._mo14s[instance].handle(msg.mo14, sender_vk)
                if isinstance(res, Handled) and res.m is not None:
                    logging.debug("ACS: delivered Mo14 for {}, {}".format(b64encode(instance), res.m))
                    self._mo14_results[instance] = res.m
                elif isinstance(res, Replay):
                    # raise AssertionError("Impossible, our Mo14 instance already instantiated")
                    return Replay()

            ones = [v for k, v in self._mo14_results.iteritems() if v == 1]
            if len(ones) >= n - t:
                difference = set(self._mo14s.keys()) - set(self._mo14_provided.keys())
                logging.debug("ACS: got n - t 1s")
                logging.debug("difference = {}".format(difference))
                for d in list(difference):
                    logging.debug("ACS: initiating BA for {}, v {}".format(b64encode(d), 0))
                    self._mo14_provided[d] = 0
                    self._mo14s[d].start(0)

            if instance not in self._mo14_provided:
                logging.debug("ACS: got BA before RBC...")
                # if we got a BA instance, but we haven't deliver its corresponding RBC,
                # we instruct the caller to replay the message
                return Replay()

        else:
            raise AssertionError("ACS: invalid payload type")

        if len(self._mo14_results) >= n:
            # return the result if we're done, otherwise return None
            assert n == len(self._mo14_results)

            self._done = True
            res = self._collate_results()
            # NOTE we just print the hash of the results and compare, the actual output is too much...
            # NOTE we also use a random value to trip up tests, since it shouldn't be used
            logging.info("ACS: DONE \"{}\""
                         .format(random.random() if self._factory.config.from_instruction else b64encode(dictionary_hash(res[0]))))
            return Handled(res)
        return Handled()

    def _collate_results(self):
        key_of_ones = [k for k, v in self._mo14_results.iteritems() if v == 1]
        res = {k: self._bracha_results[k] for k in key_of_ones}
        # logging.info("{}".format({b64encode(k): b64encode(v) for k, v in res.iteritems()}))
        return res, self._round
