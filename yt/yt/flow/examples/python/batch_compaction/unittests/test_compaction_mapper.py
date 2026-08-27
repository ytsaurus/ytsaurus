def build_events(harness, spec):
    """Build messages from (word, count) pairs, preserving order."""
    return [
        harness.build_message("events", key=harness.build_key(word=word), word=word, count=count)
        for word, count in spec
    ]


def test_merges_same_word_within_batch(harness):
    msgs = build_events(harness, [("foo", 1), ("bar", 2), ("foo", 3)])

    with harness.processing(msgs) as r:
        got = {m.payload["word"]: m.payload["count"] for m in r.messages}
        assert got == {"foo": 4, "bar": 2}


def test_each_group_carries_its_own_lineage(harness):
    msgs = build_events(harness, [("foo", 1), ("bar", 2), ("foo", 3)])

    with harness.processing(msgs) as r:
        parents_by_word = {tr.messages[0].payload["word"]: tr.parent_ids for tr in r.transform_results}
        assert parents_by_word["foo"] == [msgs[0].message_id, msgs[2].message_id]
        assert parents_by_word["bar"] == [msgs[1].message_id]


def test_grouping_is_deterministic(harness):
    msgs = build_events(harness, [("b", 1), ("a", 2), ("b", 3), ("c", 4)])

    def run():
        with harness.processing(msgs) as r:
            return [(tr.parent_ids, [m.payload["word"] for m in tr.messages]) for tr in r.transform_results]

    assert run() == run()
