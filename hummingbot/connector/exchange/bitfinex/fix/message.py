"""FIX 4.4 message framing for the Bitfinex gateway.

A FIX message is ``tag=value`` fields separated by SOH (0x01). We build the standard header
(BeginString, BodyLength, MsgType, SenderCompID, TargetCompID, MsgSeqNum, SendingTime) and the
trailer (CheckSum) ourselves; the caller supplies the message type and the body fields in the
order the dictionary wants them. No repeating-group support beyond what an ordered field list
already gives (Bitfinex's order-execution messages have none).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List, Optional, Tuple

SOH = "\x01"
BEGIN_STRING = "FIX.4.4"

TAG_BEGIN_STRING = "8"
TAG_BODY_LENGTH = "9"
TAG_CHECKSUM = "10"
TAG_MSG_SEQ_NUM = "34"
TAG_MSG_TYPE = "35"
TAG_POSS_DUP = "43"
TAG_SENDER_COMP_ID = "49"
TAG_SENDING_TIME = "52"
TAG_TARGET_COMP_ID = "56"
TAG_ORIG_SENDING_TIME = "122"

HEADER_TAGS = {TAG_BEGIN_STRING, TAG_BODY_LENGTH, TAG_MSG_TYPE, TAG_SENDER_COMP_ID, TAG_TARGET_COMP_ID,
               TAG_MSG_SEQ_NUM, TAG_SENDING_TIME, TAG_POSS_DUP, TAG_ORIG_SENDING_TIME}


class FixProtocolError(ValueError):
    """The bytes are not a well-formed FIX message (bad length, checksum, or field)."""


@dataclass
class FixMessage:
    msg_type: str
    body: List[Tuple[str, str]] = field(default_factory=list)
    # header values, filled in by decode() (and by encode() on the way out)
    sender: str = ""
    target: str = ""
    seq_num: int = 0
    sending_time: str = ""
    poss_dup: bool = False
    orig_sending_time: str = ""

    def get(self, tag: str, default: Optional[str] = None) -> Optional[str]:
        for t, v in self.body:
            if t == tag:
                return v
        return default

    def get_all(self, tag: str) -> List[str]:
        return [v for t, v in self.body if t == tag]

    def __repr__(self) -> str:  # compact, key-free: safe to log
        return f"FixMessage({self.msg_type} #{self.seq_num} {' '.join(f'{t}={v}' for t, v in self.body)})"


def _checksum(data: bytes) -> str:
    return f"{sum(data) % 256:03d}"


def encode(msg: FixMessage, sender: str, target: str, seq_num: int, sending_time: str,
           poss_dup: bool = False, orig_sending_time: str = "") -> bytes:
    """Serialise ``msg`` with a full standard header and trailer. Values must not contain SOH."""
    header: List[Tuple[str, str]] = [
        (TAG_MSG_TYPE, msg.msg_type), (TAG_SENDER_COMP_ID, sender), (TAG_TARGET_COMP_ID, target),
        (TAG_MSG_SEQ_NUM, str(seq_num)), (TAG_SENDING_TIME, sending_time),
    ]
    if poss_dup:
        header.append((TAG_POSS_DUP, "Y"))
        if orig_sending_time:
            header.append((TAG_ORIG_SENDING_TIME, orig_sending_time))
    fields: Iterable[Tuple[str, str]] = header + list(msg.body)
    parts = []
    for tag, value in fields:
        value = str(value)
        if SOH in value or SOH in tag or "=" in tag:
            raise FixProtocolError(f"field {tag} contains a delimiter")
        parts.append(f"{tag}={value}")
    body = (SOH.join(parts) + SOH).encode()
    head = f"{TAG_BEGIN_STRING}={BEGIN_STRING}{SOH}{TAG_BODY_LENGTH}={len(body)}{SOH}".encode()
    without_checksum = head + body
    return without_checksum + f"{TAG_CHECKSUM}={_checksum(without_checksum)}{SOH}".encode()


def decode(raw: bytes) -> FixMessage:
    """Parse exactly one complete message, verifying BodyLength and CheckSum."""
    text = raw.decode("ascii", errors="strict") if isinstance(raw, bytes) else raw
    if not text.startswith(f"{TAG_BEGIN_STRING}=") or not text.endswith(SOH):
        raise FixProtocolError("message does not start with BeginString or end with SOH")
    pairs = text.split(SOH)[:-1]
    fields: List[Tuple[str, str]] = []
    for pair in pairs:
        tag, sep, value = pair.partition("=")
        if not sep or not tag.isdigit():
            raise FixProtocolError(f"malformed field {pair[:20]!r}")
        fields.append((tag, value))
    if len(fields) < 4 or fields[1][0] != TAG_BODY_LENGTH or fields[2][0] != TAG_MSG_TYPE or fields[-1][0] != TAG_CHECKSUM:
        raise FixProtocolError("standard header/trailer out of order")
    raw_bytes = text.encode()
    body_start = raw_bytes.index(b"35=")
    checksum_at = raw_bytes.rindex(b"10=")
    if int(fields[1][1]) != checksum_at - body_start:
        raise FixProtocolError(f"BodyLength {fields[1][1]} does not match {checksum_at - body_start}")
    if fields[-1][1] != _checksum(raw_bytes[:checksum_at]):
        raise FixProtocolError("CheckSum mismatch")
    msg = FixMessage(fields[2][1])
    for tag, value in fields[3:-1]:
        if tag == TAG_SENDER_COMP_ID:
            msg.sender = value
        elif tag == TAG_TARGET_COMP_ID:
            msg.target = value
        elif tag == TAG_MSG_SEQ_NUM:
            msg.seq_num = int(value)
        elif tag == TAG_SENDING_TIME:
            msg.sending_time = value
        elif tag == TAG_POSS_DUP:
            msg.poss_dup = value == "Y"
        elif tag == TAG_ORIG_SENDING_TIME:
            msg.orig_sending_time = value
        else:
            msg.body.append((tag, value))
    return msg


class FixParser:
    """Turns a byte stream into complete messages; keeps the partial tail between feeds."""

    def __init__(self) -> None:
        self._buffer = b""

    def feed(self, data: bytes) -> List[FixMessage]:
        self._buffer += data
        out: List[FixMessage] = []
        while True:
            frame = self._next_frame()
            if frame is None:
                return out
            out.append(decode(frame))

    def _next_frame(self) -> Optional[bytes]:
        buf = self._buffer
        start = buf.find(b"8=")
        if start < 0:
            return None
        if start:
            buf = self._buffer = buf[start:]
        len_at = buf.find(b"\x019=")
        if len_at < 0:
            return None
        len_end = buf.find(SOH.encode(), len_at + 1)
        if len_end < 0:
            return None
        try:
            body_len = int(buf[len_at + 3:len_end])
        except ValueError:
            raise FixProtocolError("BodyLength is not a number") from None
        body_start = len_end + 1
        checksum_start = body_start + body_len
        # trailer is "10=NNN<SOH>" = 7 bytes
        if len(buf) < checksum_start + 7:
            return None
        frame = buf[:checksum_start + 7]
        self._buffer = buf[checksum_start + 7:]
        return frame
