import random
import logging
from enum import Enum

from src.utils.messages import BrachaMsg
from src.utils.utils import Handled

BrachaStep = Enum('BrachaStep', 'one two three')
MsgType = Enum('MsgType', 'init echo ready')


class Bracha:
    """
    Bracha broadcast '87
    Implemented using state machine (BrachaStep)
    """
    def __init__(self, factory, msg_wrapper_f=lambda _x: _x):
        self.factory = factory
        self.step = BrachaStep.one
        self.init_count = 0
        self.echo_count = 0
        self.ready_count = 0
        self.body = None
        self.peers_state = {}  # TODO make sure peers do not replay messages
        self.done = False
        self.msg_wrapper_f = msg_wrapper_f
        random.seed()

    def handle(self, msg):
        # type: (BrachaMsg) -> Handled
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
            logging.debug("Bracha: done, doing nothing")
            return Handled()

        ty = msg.ty
        body = msg.body
        logging.debug("Bracha: received (ty: {}, body: {})".format(ty, body))

        assert isinstance(ty, int)
        assert body is not None

        if self.body is None:
            # if body is None, we must be in the initial state
            assert self.init_count == 0 and self.echo_count == 0 and self.ready_count == 0
            self.body = body
        assert self.body == body, "self.body {} != body: {}".format(self.body, body)

        if ty == MsgType.init.value:
            self.init_count += 1
        elif ty == MsgType.echo.value:
            self.echo_count += 1
        elif ty == MsgType.ready.value:
            self.ready_count += 1
        else:
            raise AssertionError("Bracha: unexpected msg type")

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
                logging.info("Bracha: DELIVER {}".format(body))
                return Handled(body)

        return Handled()

    def bcast_init(self, msg="some test msg!!"):
        logging.info("Bracha: initiating with msg {}".format(msg))
        self.bcast(BrachaMsg(MsgType.init.value, msg))

    def bcast_echo(self, body):
        logging.debug("Bracha: broadcast echo {}".format(body))
        self.bcast(BrachaMsg(MsgType.echo.value, body))

    def bcast_ready(self, body):
        logging.debug("Bracha: broadcast ready {}".format(body))
        self.bcast(BrachaMsg(MsgType.ready.value, body))

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
        self.factory.promoter_cast(self.msg_wrapper_f(msg))
