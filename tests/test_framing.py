import random

import pytest

from siklink.framing import Frame, FrameDecoder, crc16, encode_frame


def test_standard_crc_vector():
    assert crc16(b"123456789") == 0x29B1


def test_frame_round_trip_byte_at_a_time():
    original = Frame(type=0x10, flags=1, seq=65535, payload=b"hello")
    encoded = encode_frame(original)
    decoder = FrameDecoder()
    result = []
    for byte in encoded:
        result.extend(decoder.feed(bytes([byte])))
    assert result == [original]


def test_multiple_frames_and_garbage():
    frames = [Frame(1, 0, 4, b"a"), Frame(0x10, 1, 5, b"two")]
    decoder = FrameDecoder()
    assert decoder.feed(b"garbage" + b"".join(map(encode_frame, frames))) == frames
    assert decoder.discarded_bytes == len(b"garbage")


def test_corrupt_frame_resynchronizes_to_next_frame():
    damaged = bytearray(encode_frame(Frame(0x10, 0, 1, b"bad")))
    damaged[-1] ^= 0xFF
    good = Frame(0x10, 0, 2, b"good")
    decoder = FrameDecoder()
    assert decoder.feed(bytes(damaged) + encode_frame(good)) == [good]
    assert decoder.crc_errors == 1


def test_random_chunking_and_false_magic():
    rng = random.Random(12)
    frames = [Frame(0x10, i & 1, i, bytes(rng.randrange(256) for _ in range(i % 19))) for i in range(100)]
    wire = b"\x00\xaa\x00" + b"".join(encode_frame(frame) for frame in frames)
    decoder = FrameDecoder()
    result = []
    while wire:
        size = rng.randrange(1, 17)
        result.extend(decoder.feed(wire[:size]))
        wire = wire[size:]
    assert result == frames


def test_payload_limit_is_enforced():
    with pytest.raises(ValueError, match="exceeds"):
        encode_frame(Frame(1, 0, 1, b"123"), max_payload=2)
