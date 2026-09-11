"""FIX 4.4 message framing: tag=value fields separated by SOH, BodyLength and CheckSum computed by us."""

import pytest

from hummingbot.connector.exchange.bitfinex.fix.message import FixMessage, FixParser, FixProtocolError, SOH, decode, encode


def test_encode_produces_standard_header_body_length_and_checksum():
    msg = FixMessage("0")  # Heartbeat
    raw = encode(msg, sender="MyClientComp", target="BfxComp", seq_num=1, sending_time="20260911-10:00:00.000")
    fields = raw.decode().split(SOH)
    assert fields[0] == "8=FIX.4.4"
    assert fields[1].startswith("9=")
    assert fields[2] == "35=0"
    assert "49=MyClientComp" in fields and "56=BfxComp" in fields and "34=1" in fields
    assert "52=20260911-10:00:00.000" in fields
    assert fields[-2].startswith("10=") and len(fields[-2]) == 6 and fields[-1] == ""
    # BodyLength counts every byte after the 9= field up to and including the SOH before 10=
    body_len = int(fields[1][2:])
    start = raw.index(b"35=")
    end = raw.index(b"10=")
    assert body_len == end - start


def test_checksum_is_the_byte_sum_mod_256_over_everything_before_tag_10():
    raw = encode(FixMessage("0"), sender="A", target="B", seq_num=7, sending_time="20260911-10:00:00.000")
    before_checksum = raw[: raw.index(b"10=")]
    assert raw.endswith(f"10={sum(before_checksum) % 256:03d}{SOH}".encode())


def test_decode_round_trips_fields_in_order_and_repeated_tags():
    msg = FixMessage("D", [("11", "abc"), ("55", "BTCUSD"), ("54", "1"), ("38", "0.01"), ("40", "2"), ("44", "60000"),
                           ("3927", "4096")])
    raw = encode(msg, sender="C", target="BfxComp", seq_num=3, sending_time="20260911-10:00:00.000")
    parsed = decode(raw)
    assert parsed.msg_type == "D"
    assert parsed.get("11") == "abc" and parsed.get("3927") == "4096" and parsed.get("44") == "60000"
    assert parsed.seq_num == 3 and parsed.sender == "C" and parsed.target == "BfxComp"
    assert [t for t, _ in parsed.body] == ["11", "55", "54", "38", "40", "44", "3927"]


def test_decode_refuses_a_bad_checksum_and_a_bad_body_length():
    raw = encode(FixMessage("0"), sender="A", target="B", seq_num=1, sending_time="20260911-10:00:00.000")
    bad = raw[:-4] + b"000" + SOH.encode()
    with pytest.raises(FixProtocolError):
        decode(bad)
    with pytest.raises(FixProtocolError):
        decode(raw.replace(b"9=", b"9=9"))


def test_parser_splits_a_stream_into_complete_messages_only():
    a = encode(FixMessage("0"), sender="A", target="B", seq_num=1, sending_time="20260911-10:00:00.000")
    b = encode(FixMessage("1", [("112", "x")]), sender="A", target="B", seq_num=2, sending_time="20260911-10:00:00.000")
    parser = FixParser()
    assert parser.feed(a[:10]) == []
    first = parser.feed(a[10:] + b[:5])
    assert [m.msg_type for m in first] == ["0"]
    second = parser.feed(b[5:])
    assert [m.msg_type for m in second] == ["1"] and second[0].get("112") == "x"


def test_values_containing_soh_are_refused_at_encode_time():
    with pytest.raises(FixProtocolError):
        encode(FixMessage("0", [("58", "a" + SOH + "b")]), sender="A", target="B", seq_num=1,
               sending_time="20260911-10:00:00.000")
