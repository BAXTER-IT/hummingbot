"""The FIX session against the Bitfinex simulator: logon, heartbeats, sequence recovery, orders, session end."""

import asyncio

import pytest
import pytest_asyncio

from hummingbot.connector.exchange.bitfinex.fix.message import FixMessage
from hummingbot.connector.exchange.bitfinex.fix.session import FixLogonError, FixSession, SessionState
from test.hummingbot.connector.exchange.bitfinex.fix.bitfinex_simulator import BitfinexFixSimulator

pytestmark = pytest.mark.asyncio


class Sink:
    def __init__(self):
        self.app = []
        self.states = []
        self.app_event = asyncio.Event()

    def on_app(self, msg):
        self.app.append(msg)
        self.app_event.set()

    def on_state(self, state, text):
        self.states.append((state, text))

    async def wait_app(self, n, timeout=5):
        async def until():
            while len(self.app) < n:
                self.app_event.clear()
                await self.app_event.wait()
        await asyncio.wait_for(until(), timeout)


@pytest_asyncio.fixture
async def sim():
    simulator = BitfinexFixSimulator(username="fxch", password="s3cret")
    await simulator.start()
    yield simulator
    await simulator.stop()


def make_session(sim, sink, **over):
    host, port = sim._server.sockets[0].getsockname()[:2]
    kwargs = dict(host=host, port=port, sender_comp_id="FXCHTEST", target_comp_id="BfxComp", username="fxch",
                  password="s3cret", heartbeat_s=1.0, use_tls=False, on_app_message=sink.on_app,
                  on_state=sink.on_state, log=lambda _: None)
    kwargs.update(over)
    return FixSession(**kwargs)


async def test_logon_handshake_sends_credentials_and_reports_logged_on(sim):
    sink = Sink()
    session = make_session(sim, sink)
    await session.connect()
    try:
        assert session.state is SessionState.LOGGED_ON
        logon = sim.received[0]
        assert logon.msg_type == "A" and logon.get("553") == "fxch" and logon.get("554") == "s3cret"
        assert logon.get("108") == "1" and logon.get("98") == "0"
        assert (SessionState.LOGGED_ON, "") in sink.states
    finally:
        await session.close()


async def test_wrong_password_is_a_logon_error_with_the_venues_text(sim):
    sink = Sink()
    session = make_session(sim, sink, password="wrong")
    with pytest.raises(FixLogonError) as err:
        await session.connect()
    assert "Invalid credentials" in str(err.value)
    assert session.state is SessionState.DISCONNECTED


async def test_new_order_single_gets_an_execution_report_back_through_the_app_callback(sim):
    sink = Sink()
    session = make_session(sim, sink)
    await session.connect()
    try:
        seq = await session.send(FixMessage("D", [("11", "c1"), ("55", "BTCUST"), ("54", "1"), ("38", "0.001"),
                                                  ("40", "2"), ("44", "60000"), ("59", "0"), ("3927", "4096")]))
        assert seq == 2  # logon was 1
        await sink.wait_app(1)
        er = sink.app[0]
        assert er.msg_type == "8" and er.get("11") == "c1" and er.get("150") == "0" and er.get("3927") == "4096"
        assert er.seq_num == 2
    finally:
        await session.close()


async def test_a_fill_and_a_cancel_arrive_as_execution_reports(sim):
    sink = Sink()
    session = make_session(sim, sink)
    await session.connect()
    try:
        sim.fill_next = True
        await session.send(FixMessage("D", [("11", "c2"), ("55", "BTCUST"), ("54", "2"), ("38", "0.5"), ("40", "2"),
                                            ("44", "61000")]))
        await sink.wait_app(2)
        assert [m.get("150") for m in sink.app] == ["0", "F"]
        assert sink.app[1].get("32") == "0.5" and sink.app[1].get("1057") == "N"
        await session.send(FixMessage("F", [("11", "c2-x"), ("41", "c2")]))
        await sink.wait_app(3)
        assert sink.app[2].msg_type == "9" and sink.app[2].get("102") == "0"  # too late: already filled
    finally:
        await session.close()


async def test_heartbeats_flow_and_test_requests_are_answered(sim):
    sink = Sink()
    session = make_session(sim, sink, heartbeat_s=0.2)
    await session.connect()
    try:
        await sim.wait_for(lambda r: any(m.msg_type == "0" for m in r), timeout=3)
        await sim.send_test_request("abc")
        await sim.wait_for(lambda r: any(m.msg_type == "0" and m.get("112") == "abc" for m in r), timeout=3)
    finally:
        await session.close()


async def test_a_gap_in_their_numbering_triggers_our_resend_request_and_the_replay_is_delivered_once(sim):
    sink = Sink()
    session = make_session(sim, sink)
    await session.connect()
    try:
        sim.skip_next_seq = True
        await session.send(FixMessage("D", [("11", "c3"), ("55", "BTCUST"), ("54", "1"), ("38", "1"), ("40", "2"),
                                            ("44", "1")]))
        await sim.wait_for(lambda r: any(m.msg_type == "2" for m in r), timeout=3)
        resend = next(m for m in sim.received if m.msg_type == "2")
        assert resend.get("7") == "2" and resend.get("16") == "0"
        await sink.wait_app(1)
        await asyncio.sleep(0.3)
        assert [m.get("11") for m in sink.app] == ["c3"]
        assert sink.app[0].poss_dup is True
    finally:
        await session.close()


async def test_their_resend_request_is_answered_with_a_gap_fill(sim):
    sink = Sink()
    session = make_session(sim, sink)
    await session.connect()
    try:
        await session.send(FixMessage("D", [("11", "c4"), ("55", "BTCUST"), ("54", "1"), ("38", "1"), ("40", "2"),
                                            ("44", "1")]))
        await sim.send_resend_request(begin=1, end=0)
        await sim.wait_for(lambda r: any(m.msg_type == "4" for m in r), timeout=3)
        gap_fill = next(m for m in sim.received if m.msg_type == "4")
        assert gap_fill.get("123") == "Y" and int(gap_fill.get("36")) == 3 and gap_fill.poss_dup is True
    finally:
        await session.close()


async def test_server_logout_ends_the_session_with_its_text_and_orders_are_reported_gone(sim):
    sink = Sink()
    session = make_session(sim, sink)
    await session.connect()
    try:
        await sim.send_pre_close()
        await sink.wait_app(1)
        assert sink.app[0].msg_type == "h" and sink.app[0].get("340") == "5"
        await sim.send_logout("Trading session ending")
        for _ in range(50):
            if session.state is SessionState.DISCONNECTED:
                break
            await asyncio.sleep(0.05)
        assert session.state is SessionState.DISCONNECTED
        assert any(s is SessionState.LOGGED_OUT and "session ending" in t for s, t in sink.states)
    finally:
        await session.close()


async def test_reconnect_within_the_day_continues_sequence_numbers(sim):
    sink = Sink()
    store = {}
    session = make_session(sim, sink, seq_store=store)
    await session.connect()
    await session.send(FixMessage("D", [("11", "c5"), ("55", "BTCUST"), ("54", "1"), ("38", "1"), ("40", "2"), ("44", "1")]))
    await sink.wait_app(1)
    await sim.drop_connection()
    for _ in range(50):
        if session.state is SessionState.DISCONNECTED:
            break
        await asyncio.sleep(0.05)
    session2 = make_session(sim, sink, seq_store=store)
    await session2.connect()
    try:
        logon2 = [m for m in sim.received if m.msg_type == "A"][1]
        assert logon2.seq_num == 3 and logon2.get("141", "N") == "N"
    finally:
        await session2.close()


async def test_silence_after_a_test_request_disconnects(sim):
    sink = Sink()
    session = make_session(sim, sink, heartbeat_s=0.2)
    await session.connect()
    sim.mute = True  # the simulator stops answering
    original = sim._on_message

    async def swallow(msg):
        sim.received.append(msg)

    sim._on_message = swallow
    try:
        for _ in range(60):
            if session.state is SessionState.DISCONNECTED:
                break
            await asyncio.sleep(0.05)
        assert session.state is SessionState.DISCONNECTED
        assert any(m.msg_type == "1" for m in sim.received)  # we did ask
    finally:
        sim._on_message = original
        await session.close()
