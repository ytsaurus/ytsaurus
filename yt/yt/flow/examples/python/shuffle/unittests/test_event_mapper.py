import json


def test_parses_json_and_emits_event(mapper_harness):
    data = json.dumps({"value": "v1", "key_a": "a", "key_b": "b", "key_c": "c", "key_d": "d"})
    msg = mapper_harness.build_message("input", data=data)

    with mapper_harness.processing([msg]) as r:
        assert len(r.messages) == 1
        out = r.messages[0]
        assert out.payload["value"] == "v1"
        assert out.payload["key_a"] == "a"
        assert out.payload["key_b"] == "b"
        assert out.payload["key_c"] == "c"
        assert out.payload["key_d"] == "d"


def test_multiple_messages_emit_multiple_events(mapper_harness):
    msgs = []
    for i in range(5):
        data = json.dumps({"value": f"v{i}", "key_a": "a", "key_b": "b", "key_c": "c", "key_d": "d"})
        msgs.append(mapper_harness.build_message("input", data=data))

    with mapper_harness.processing(msgs) as r:
        assert len(r.messages) == 5
        for i, out in enumerate(r.messages):
            assert out.payload["value"] == f"v{i}"
