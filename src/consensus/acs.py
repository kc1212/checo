from base64 import b64encode
from typing import Dict, Union
import json

from .bracha import Bracha
from .mo14 import Mo14
from src.utils.messages import ACSMsg, BrachaMsg, Mo14Msg
from src.utils.utils import Replay, Handled


class ACS:
    def __init__(self, factory):
        self.factory = factory
        self.round = 0
        self.done = False
        # the following are initialised at start
        self.brachas = {}  # type: Dict[str, Bracha]
        self.mo14s = {}  # type: Dict[str, Mo14]
        self.bracha_results = {}  # type: Dict[str, str]
        self.mo14_results = {}  # type: Dict[str, int]
        self.mo14_provided = {}  # type: Dict[str, int]

    def start(self, msg):
        # initialise our RBC and BA instances
        # assume all the peers are connected
        assert len(self.factory.peers) == self.factory.config.n
        for peer in self.factory.peers.keys():
            print "ACS: adding peer", b64encode(peer)

            # TODO when do we update the round?
            def msg_wrapper_f_factory(instance, round):
                def f(_msg):
                    return ACSMsg(instance, round, _msg)
                return f

            self.brachas[peer] = Bracha(self.factory, msg_wrapper_f_factory(peer, self.round))
            self.mo14s[peer] = Mo14(self.factory, msg_wrapper_f_factory(peer, self.round))

        my_vk = self.factory.vk
        assert my_vk in self.brachas
        assert my_vk in self.mo14s

        # send the first RBC, assume all nodes have connected
        print "ACS: initiating...", b64encode(my_vk), msg
        self.brachas[my_vk].bcast_init(msg)

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
        print "ACS: got msg (instance: {}, round: {}) from {}".format(b64encode(msg.instance), msg.round, b64encode(sender_vk))
        if self.done:
            print "ACS: we're done, doing nothing"
            return Handled()

        instance = msg.instance
        round = msg.round
        assert round == self.round
        body = msg.body
        t = self.factory.config.t
        n = self.factory.config.n

        if isinstance(body, BrachaMsg):
            if instance not in self.brachas:
                print "instance {} not in self.brachas".format(b64encode(instance))
                return Replay()
            res = self.brachas[instance].handle(body)
            if isinstance(res, Handled) and res.m is not None:
                print "ACS: Bracha delivered", b64encode(instance), res.m
                self.bracha_results[instance] = res.m
                if instance not in self.mo14_provided:
                    print "ACS: initiating BA", b64encode(instance), 1
                    self.mo14_provided[instance] = 1
                    self.mo14s[instance].start(1)

        elif isinstance(body, Mo14Msg):
            if instance in self.mo14_provided:
                print "ACS: forwarding Mo14"
                res = self.mo14s[instance].handle(body, sender_vk)
                if isinstance(res, Handled) and res.m is not None:
                    print "ACS: delivered Mo14", b64encode(instance), res.m
                    self.mo14_results[instance] = res.m
                elif isinstance(res, Replay):
                    # raise AssertionError("Impossible, our Mo14 instance already instantiated")
                    return Replay()

            ones = [v for k, v in self.mo14_results.iteritems() if v == 1]
            if len(ones) >= n - t:
                difference = set(self.mo14s.keys()) - set(self.mo14_provided.keys())
                print "ACS: got n - t 1s"
                print "difference =", difference
                for d in list(difference):
                    print "ACS: initiating BA", d, 0
                    self.mo14_provided[d] = 0
                    self.mo14s[d].start(0)

            if instance not in self.mo14_provided:
                print "ACS: got BA before RBC..."
                # if we got a BA instance, but we haven't deliver its corresponding RBC,
                # we instruct the caller to replay the message
                return Replay()

        else:
            raise AssertionError("ACS: invalid payload type")

        if len(self.mo14_results) >= n:
            # return the result if we're done, otherwise return None
            assert n == len(self.mo14_results)

            self.done = True
            print "ACS: DONE", json.dumps(self.get_results())
            return Handled(self.mo14_results)
        return Handled()

    def get_results(self):
        res = {'set': {b64encode(k): v for k, v in self.mo14_results.iteritems()},
               'msgs': {b64encode(k): v for k, v in self.bracha_results.iteritems()}}
        return res
