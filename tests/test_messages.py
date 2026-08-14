import pytest

from siklink.messages import pack_user_message, unpack_user_message


def test_user_message_round_trip_preserves_binary_types():
    payload = pack_user_message("illumination", {"intensity": 0.65, "raw": b"x"})
    assert unpack_user_message(payload) == {
        "topic": "illumination",
        "data": {"intensity": 0.65, "raw": b"x"},
    }


@pytest.mark.parametrize("topic", ["", None, 42])
def test_topic_must_be_nonempty_string(topic):
    with pytest.raises(ValueError):
        pack_user_message(topic, {})
