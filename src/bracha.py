from enum import Enum
from messages import Payload
from utils import bcolors
import random

BrachaStep = Enum('BrachaStep', 'one two three')
MsgType = Enum('MsgType', 'init echo ready')


class Bracha:
    def __init__(self, factory):
        self.factory = factory
        self.step = BrachaStep.one
        self.round = -1
        self.init_count = 0
        self.echo_count = 0
        self.ready_count = 0
        self.body = None  # TODO make sure the bodies match
        self.peers_state = {}  # TODO make sure peers do not replay messages
        self.done = False
        random.seed()

    def handle(self, msg):
        """
        This function is called on a new incoming message, we expect the type is correct
        msg should be in the following format
        struct Msg {
            ty: u32,
            round: u32,
            body: String,
        }
        :param msg:
        :return:
        """
        if self.done:
            print "Bracha is done, doing nothing"
            return


        print "received: ", msg
        ty = msg["ty"]
        round = msg["round"]
        body = msg["body"]

        assert isinstance(round, int)
        assert isinstance(ty, int)

        if round < 0:
            print "bracha: invalid round", round
            return

        if ty != MsgType.init.value and round != self.round:
            # NOTE this is often ok, since the value may already be accepted
            print "bracha: found invalid message", msg
            return

        if ty == MsgType.init.value:
            self.round = round  # TODO make sure rounds do not overlap
            self.init_count += 1
        elif ty == MsgType.echo.value:
            self.echo_count += 1
        elif ty == MsgType.ready.value:
            self.ready_count += 1
        else:
            print "bracha: unexpected msg type: ", msg["ty"]
            raise AssertionError

        assert (self.init_count == 0 or self.init_count == 1)

        if self.step == BrachaStep.one:
            if self.init_count > 0 or self.ok_to_send():
                self.bcast_echo(body)
                self.step = BrachaStep.two
        if self.step == BrachaStep.two:
            if self.ok_to_send():
                self.bcast_ready(body)
                self.step = BrachaStep.three
        if self.step == BrachaStep.three:
            if self.enough_ready():
                self.step = BrachaStep.one
                self.round = -1
                self.init_count = 0
                self.done = True
                print bcolors.OKGREEN + "bracha: ACCEPT" + bcolors.ENDC, body

    def bcast_init(self):
        self.round = random.randint(0, 9999)
        self.bcast(make_init(self.round, "agree this " + str(random.random())))
        print "initiating round", self.round

    def bcast_echo(self, body):
        self.bcast(make_echo(self.round, body))

    def bcast_ready(self, body):
        self.bcast(make_ready(self.round, body))

    def ok_to_send(self):
        n = self.factory.config.n
        t = self.factory.config.t

        if self.echo_count >= (n + t) / 2 or self.ready_count >= (t + 1):
            return True
        return False

    def enough_ready(self):
        t = self.factory.config.t

        if self.ready_count >= 2 * t + 1:
            return True
        return False

    def bcast(self, msg):
        print "broadcast:", msg["payload"]
        self.factory.bcast(msg)


def make_init(round, body):
    return _make_msg(MsgType.init.value, round, body)


def make_echo(round, body):
    return _make_msg(MsgType.echo.value, round, body)


def make_ready(round, body):
    return _make_msg(MsgType.ready.value, round, body)


def _make_msg(ty, round, body):
    return Payload.make_bracha({"ty": ty, "round": round, "body": body}).to_dict()
