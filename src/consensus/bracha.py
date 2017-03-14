import random

from enum import Enum
from src.utils.messages import Payload

from src.utils.utils import Handled

BrachaStep = Enum('BrachaStep', 'one two three')
MsgType = Enum('MsgType', 'init echo ready')


class Bracha:
    """
    Bracha broadcast '87
    Implemented using state machine (BrachaStep)
    """
    def __init__(self, factory, acs_hdr_f=lambda _x: _x):
        self.factory = factory
        self.step = BrachaStep.one
        self.init_count = 0
        self.echo_count = 0
        self.ready_count = 0
        self.body = None  # TODO make sure the bodies match
        self.peers_state = {}  # TODO make sure peers do not replay messages
        self.done = False
        self.acs_hdr_f = acs_hdr_f
        random.seed()

    def handle(self, msg):
        """
        This function is called on a new incoming message, we expect the type is correct
        msg should be in the following format
        struct Msg {
            ty: u32,
            body: String,
        }
        :param msg: the input message to send
        :return: the delivered message when completed, otherwise None
        """
        if self.done:
            print "Bracha: done, doing nothing"
            return Handled()

        print "Bracha: received", msg
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
            print "Bracha: unexpected msg type", msg["ty"]
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
                print "Bracha: DELIVER", body
                return Handled(body)

        return Handled()

    def bcast_init(self, msg="some test msg!!"):
        print "Bracha: initiating with msg", msg
        self.bcast(make_init(msg))

    def bcast_echo(self, body):
        print "Bracha: broadcast echo", body
        self.bcast(make_echo(body))

    def bcast_ready(self, body):
        print "Bracha: broadcast ready", body
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
        self.factory.bcast(self.acs_hdr_f(msg))


def make_init(body):
    return _make_msg(MsgType.init.value, body)


def make_echo(body):
    return _make_msg(MsgType.echo.value, body)


def make_ready(body):
    return _make_msg(MsgType.ready.value, body)


def _make_msg(ty, body):
    return Payload.make_bracha({"ty": ty, "body": body}).to_dict()
