def test_first_event_initializes_count(reducer_harness):
    key = reducer_harness.build_key(key_a="a", key_b="b")
    msg = reducer_harness.build_message(
        "event",
        key=key,
        value="v",
        key_a="a",
        key_b="b",
        key_c="c",
        key_d="d",
    )

    with reducer_harness.processing([msg]) as r:
        assert r.external_state("/shuffle-state", key)["count"] == 1


def test_multiple_events_increment_count(reducer_harness):
    key = reducer_harness.build_key(key_a="a", key_b="b")
    msgs = [
        reducer_harness.build_message(
            "event",
            key=key,
            value="v",
            key_a="a",
            key_b="b",
            key_c="c",
            key_d="d",
        )
        for _ in range(4)
    ]

    with reducer_harness.processing(msgs) as r:
        assert r.external_state("/shuffle-state", key)["count"] == 4


def test_different_keys_have_separate_counts(reducer_harness):
    k1 = reducer_harness.build_key(key_a="a", key_b="1")
    k2 = reducer_harness.build_key(key_a="a", key_b="2")
    msgs = [
        reducer_harness.build_message("event", key=k1, value="v", key_a="a", key_b="1", key_c="c", key_d="d"),
        reducer_harness.build_message("event", key=k2, value="v", key_a="a", key_b="2", key_c="c", key_d="d"),
        reducer_harness.build_message("event", key=k1, value="v", key_a="a", key_b="1", key_c="c", key_d="d"),
    ]

    with reducer_harness.processing(msgs) as r:
        assert r.external_state("/shuffle-state", k1)["count"] == 2
        assert r.external_state("/shuffle-state", k2)["count"] == 1
