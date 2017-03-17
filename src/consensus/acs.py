from base64 import b64encode, b64decode
from twisted.internet import reactor

from .bracha import Bracha
from .mo14 import Mo14
from src.utils.messages import PayloadType, Payload
from src.utils.utils import Replay, Handled


class ACS:
    def __init__(self, factory):
        self.factory = factory
        self.round = 0
        self.done = False
        # the following are initialised at start
        self.brachas = {}  # key: vk, value: Bracha (aka RBC)
        self.mo14s = {}  # key: vk, value: Mo14 (aka BA)
        self.bracha_results = {}
        self.mo14_results = {}
        self.mo14_provided = {}

    def start(self, msg):
        # initialise our RBC and BA instances
        # assume all the peers are connected
        assert len(self.factory.peers) == self.factory.config.n
        for peer in self.factory.peers.keys():
            print "ACS: adding peer", b64encode(peer)

            # TODO when do we update the round?
            def acs_hdr_f_factory(instance, ty, round):
                def f(_msg):
                    return Payload.make_acs({"instance": b64encode(instance),
                                             "ty": ty,
                                             "round": round,
                                             "body": _msg['payload']
                                             }).to_dict()
                return f

            self.brachas[peer] = Bracha(self.factory, acs_hdr_f_factory(peer, PayloadType.bracha.value, self.round))
            self.mo14s[peer] = Mo14(self.factory, acs_hdr_f_factory(peer, PayloadType.mo14.value, self.round))

        my_vk = self.factory.vk
        assert my_vk in self.brachas
        assert my_vk in self.mo14s

        # send the first RBC, assume all nodes have connected
        print "ACS: initiating...", b64encode(my_vk), msg
        self.brachas[my_vk].bcast_init(msg)

    def handle(self, msg, sender_vk):
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
        if self.done:
            print "ACS: we're done, doing nothing", msg, sender_vk
            return Handled()

        instance = b64decode(msg["instance"])
        ty = msg["ty"]
        # TODO check round
        round = msg["round"]
        assert round == self.round
        body = msg["body"]
        t = self.factory.config.t
        n = self.factory.config.n

        print "ACS: got msg", msg, "from", b64encode(sender_vk)

        if ty == PayloadType.bracha.value:
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

        elif ty == PayloadType.mo14.value:
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
            raise AssertionError("ACS: invalid payload type - {}".format(ty))

        if len(self.mo14_results) >= n:
            import json
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
