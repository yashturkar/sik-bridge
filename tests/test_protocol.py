from collections import deque

from siklink.config import ProtocolConfig
from siklink.metrics import LinkState
from siklink.protocol import ProtocolEngine


class Clock:
    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def make_engine(clock, outgoing, events, **overrides):
    config = ProtocolConfig(**overrides)
    return ProtocolEngine(
        config,
        lambda data, priority, _key=None: outgoing.append((priority, data)) or True,
        lambda name, payload: events.append((name, payload)),
        clock=clock,
        wall_clock=lambda: 1000.0,
        initial_seq=10,
    )


def transfer(outgoing, receiver):
    while outgoing:
        _, data = outgoing.popleft()
        receiver.receive(data)


def test_reliable_message_is_delivered_and_acked():
    clock = Clock()
    a_out, b_out, a_events, b_events = deque(), deque(), [], []
    a = make_engine(clock, a_out, a_events)
    b = make_engine(clock, b_out, b_events)
    seq = a.send_user("commands", {"go": True}, reliable=True, context="request")
    transfer(a_out, b)
    transfer(b_out, a)
    assert ("message", {"topic": "commands", "data": {"go": True}, "seq": seq, "reliable": True}) in b_events
    result = [payload for name, payload in a_events if name == "send_result"]
    assert result == [{"seq": seq, "ok": True, "reliable": True, "context": "request", "error": None}]


def test_duplicate_reliable_message_is_not_redelivered_but_is_acked_twice():
    clock = Clock()
    outgoing, events = deque(), []
    sender_out = deque()
    sender = make_engine(clock, sender_out, [])
    receiver = make_engine(clock, outgoing, events)
    sender.send_user("x", 1, reliable=True)
    frame = sender_out[0][1]
    receiver.receive(frame)
    receiver.receive(frame)
    assert len([event for event in events if event[0] == "message"]) == 1
    assert receiver.metrics.duplicates == 1
    assert len(outgoing) == 2


def test_reliable_send_retries_then_times_out():
    clock = Clock()
    outgoing, events = deque(), []
    engine = make_engine(clock, outgoing, events, ack_timeout=0.3, max_retries=2)
    engine.metrics.last_valid_rx = clock()
    engine.send_user("x", 1, reliable=True, context=5)
    for _ in range(3):
        clock.advance(0.31)
        engine.tick()
    results = [payload for name, payload in events if name == "send_result"]
    assert engine.metrics.retries == 2
    assert results[-1]["error"] == "timeout"
    assert results[-1]["context"] == 5


def test_heartbeat_calculates_rtt_and_connected_state():
    clock = Clock()
    a_out, b_out = deque(), deque()
    a = make_engine(clock, a_out, [])
    b = make_engine(clock, b_out, [])
    a.tick()
    transfer(a_out, b)
    clock.advance(0.04)
    transfer(b_out, a)
    assert round(a.metrics.rtt_ms) == 40
    assert a.status()["state"] == LinkState.CONNECTED.value
    clock.advance(3.1)
    assert a.status()["state"] == LinkState.DISCONNECTED.value


def test_heartbeat_reply_can_arrive_after_next_transmit_interval():
    clock = Clock()
    a_out, b_out = deque(), deque()
    a = make_engine(clock, a_out, [], heartbeat_interval=1.0, heartbeat_reply_timeout=2.0)
    b = make_engine(clock, b_out, [], heartbeat_interval=10.0, heartbeat_phase=10.0)
    a.tick()
    transfer(a_out, b)
    clock.advance(1.2)
    a.tick()
    transfer(b_out, a)
    assert a.metrics.heartbeat_loss == 0.0
    assert round(a.metrics.rtt_ms) == 1200


def test_degraded_state_requires_samples_and_hysteresis():
    clock = Clock()
    engine = make_engine(
        clock, deque(), [], minimum_health_samples=3,
        degraded_enter_samples=2, degraded_exit_samples=3,
    )
    engine.metrics.last_valid_rx = clock()
    for result in (True, False, False, False):
        engine._record_heartbeat(result)
    assert engine.status()["state"] == LinkState.DEGRADED.value
    for _ in range(2):
        engine._record_heartbeat(True)
    assert engine.status()["state"] == LinkState.DEGRADED.value
    # Clear the rolling loss before counting three fully healthy samples.
    for _ in range(35):
        engine._record_heartbeat(True)
    assert engine.status()["state"] == LinkState.CONNECTED.value


def test_sequence_wraps():
    clock = Clock()
    outgoing = deque()
    engine = ProtocolEngine(
        ProtocolConfig(), lambda data, priority, _key=None: outgoing.append(data) or True, lambda *_: None,
        clock=clock, initial_seq=0xFFFF,
    )
    assert engine.send_user("x", 1) == 0xFFFF
    assert engine.send_user("x", 2) == 0


def test_disconnect_fails_pending_reliable_send():
    clock = Clock()
    outgoing, events = deque(), []
    engine = make_engine(clock, outgoing, events)
    engine.metrics.last_valid_rx = clock()
    engine.send_user("command", {"op": "stop"}, reliable=True, context="stop")
    engine.tick()
    clock.advance(3.1)
    engine.tick()
    result = [payload for name, payload in events if name == "send_result"][-1]
    assert result["context"] == "stop"
    assert result["error"] == "disconnected"
