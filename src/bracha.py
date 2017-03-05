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
        body = msg["body"]

        assert isinstance(ty, int)

        if ty == MsgType.init.value:
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
                self.init_count = 0
                self.done = True
                print bcolors.OKGREEN + "bracha: ACCEPT" + bcolors.ENDC, body

    def bcast_init(self, msg="some test msg!!"):
        self.bcast(make_init(msg))
        print "initiating with msg", msg

    def bcast_echo(self, body):
        self.bcast(make_echo(body))

    def bcast_ready(self, body):
        self.bcast(make_ready(body))

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


def make_init(body):
    return _make_msg(MsgType.init.value, body)


def make_echo(body):
    return _make_msg(MsgType.echo.value, body)


def make_ready(body):
    return _make_msg(MsgType.ready.value, body)


def _make_msg(ty, body):
    return Payload.make_bracha({"ty": ty, "body": body}).to_dict()
