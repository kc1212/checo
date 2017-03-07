import uuid
from twisted.internet import reactor
from bracha import Bracha
from mo14 import Mo14
from messages import PayloadType, Payload


class ACS:
    def __init__(self, factory):
        self.factory = factory
        self.round = 0
        self.done = False
        # the following are initialised at start
        self.brachas = {}  # key: uuid, value: Bracha (aka RBC)
        self.mo14s = {}  # key: uuid, value: Mo14 (aka BA)
        self.bracha_results = {}
        self.mo14_results = {}
        self.mo14_provided = {}
        self.unprocessed = []

    def start(self, msg):
        # initialise our RBC and BA instances
        # assume all the peers are connected
        assert len(self.factory.peers) == self.factory.config.n
        for peer in self.factory.peers.keys():
            print "ACS: adding peer", peer, type(peer)

            # TODO when do we update the round?
            def acs_hdr_f_factory(instance, ty, round):
                def f(msg):
                    return Payload.make_acs({"instance": instance, "ty": ty, "round": round, "body": msg}).to_dict()

                return f

            self.brachas[peer] = Bracha(self.factory, acs_hdr_f_factory(peer.urn, PayloadType.bracha.value, self.round))
            self.mo14s[peer] = Mo14(self.factory, acs_hdr_f_factory(peer.urn, PayloadType.mo14.value, self.round))

        uuid_i = self.factory.config.id
        assert uuid_i in self.brachas
        assert uuid_i in self.mo14s

        # send the first RBC, assume all nodes have connected
        print "ACS: initiating...", uuid_i, msg
        reactor.callLater(5, self.brachas[uuid_i].bcast_init, msg)

    def handle(self, msg, sender_uuid):
        """
        Msg {
            instance: String // uuid
            ty: u32
            round: u32 // this is not the same as the Mo14 'r'
            body: Bracha | Mo14 // defined by ty
        }
        :param msg: acs header with uuid followed by either a 'bracha' message or a 'mo14' message
        :param sender_uuid: the uuid of the sender
        :return: the agreed subset on completion otherwise None
        """
        if self.done:
            print "ACS: we're done, doing nothing", msg, sender_uuid
            return None

        instance = uuid.UUID(msg["instance"])
        ty = msg["ty"]
        # TODO check round
        round = msg["round"]
        assert round == self.round
        body = msg["body"]
        t = self.factory.config.t
        n = self.factory.config.n

        print "ACS: got msg", msg, "from", sender_uuid

        if ty == PayloadType.bracha.value:
            res = self.brachas[instance].handle(body)
            if res is not None:
                print "ACS: Bracha delivered", instance, res
                self.bracha_results[instance] = res
                if instance not in self.mo14_provided:
                    print "ACS: initiating BA", instance, 1
                    self.mo14_provided[instance] = 1
                    self.mo14s[instance].delayed_start(1)

        elif ty == PayloadType.mo14.value:
            if instance in self.mo14_provided:
                print "ACS: forwarding Mo14"
                res = self.mo14s[instance].handle(body, sender_uuid)
                if res is not None:
                    print "ACS: delivered Mo14", instance, res
                    self.mo14_results[instance] = res

            ones = [v for k, v in self.mo14_results.iteritems() if v == 1]
            if len(ones) >= n - t:
                print "ACS: got n - t 1s"
                difference = set(self.mo14s.keys()) - set(self.mo14_provided.keys())
                for d in list(difference):
                    print "ACS: initiating BA", d, 0
                    self.mo14_provided[d] = 0
                    self.mo14s[d].delayed_start(0)

            if instance not in self.mo14_provided:
                print "ACS: got BA before RBC..."
                # if we got a BA instance, but we haven't deliver its corresponding RBC, we do nothing
                # we process it later?
                reactor.callLater(1, self.handle, msg, sender_uuid)

        else:
            print "ACS: invalid payload type", ty
            raise AssertionError

        if len(self.mo14_results) >= n:
            import json
            # return the result if we're done, otherwise return None
            assert n == len(self.mo14_results)
            self.done = True
            res = {'set': self.mo14_results.values(), 'msgs': self.bracha_results.values()}
            print "ACS: DONE", json.dumps(res)
            return self.mo14_results
        return None
