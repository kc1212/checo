from enum import Enum

BrachaStep = Enum('BrachaStep', 'one two three')
MsgType = Enum('MsgType', 'init echo ready')

class Bracha:
    def __init__(self, peers, config):
        self.peers = peers
        self.config = config
        self.step = BrachaStep.one
        self.round = -1
        self.init_count = 0
        self.echo_count = 0
        self.ready_count = 0
        self.body = None

    def process(self, msg):
        print("received: ", msg)
        ty = msg["ty"]
        round = msg["round"]
        body = msg["body"]

        if round < 0:
            print("invalid round", round)
            return

        if ty != MsgType.init.value and round != self.round:
            print("found invalid message", msg)
            return

        if ty == MsgType.init.value:
            self.round = round  # TODO make sure rounds do not overlap
            self.init_count += 1
        elif ty == MsgType.echo.value:
            self.echo_count += 1
        elif ty == MsgType.ready.value:
            self.ready_count += 1
        else:
            print("unexpected msg type: ", msg["ty"])
            raise

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
                print("accept", body)

    def bcast_echo(self, body):
        for p in self.peers.itervalues():
            p.sendJSON(make_echo(self.round, body))

    def bcast_ready(self, body):
        for p in self.peers.itervalues():
            p.sendJSON(make_ready(self.round, body))

    def ok_to_send(self):
        if self.echo_count >= (self.config.n + self.config.t) / 2 or self.ready_count >= (self.config.t + 1):
            return True
        return False

    def enough_ready(self):
        if self.ready_count >= 2 * self.config.t + 1:
            return True
        return False


def make_init(round, body):
    return _make_msg(MsgType.init.value, round, body)


def make_echo(round, body):
    return _make_msg(MsgType.echo.value, round, body)


def make_ready(round, body):
    return _make_msg(MsgType.ready.value, round, body)


def _make_msg(ty, round, body):
    return {"ty": ty, "round": round, "body": body}

