def test_single_word_increments_count(harness):
    key = harness.build_key(word="hello")
    msg = harness.build_message("words", key=key, word="hello")

    with harness.processing([msg]) as r:
        state = r.internal_state("word-state", key)
        assert state["word"] == "hello"
        assert state["count"] == 1


def test_repeated_words_accumulate(harness):
    key = harness.build_key(word="hello")
    msgs = [harness.build_message("words", key=key, word="hello") for _ in range(3)]

    with harness.processing(msgs) as r:
        assert r.internal_state("word-state", key)["count"] == 3


def test_different_words_have_separate_state(harness):
    k1 = harness.build_key(word="foo")
    k2 = harness.build_key(word="bar")
    msgs = [
        harness.build_message("words", key=k1, word="foo"),
        harness.build_message("words", key=k2, word="bar"),
        harness.build_message("words", key=k1, word="foo"),
    ]

    with harness.processing(msgs) as r:
        assert r.internal_state("word-state", k1)["count"] == 2
        assert r.internal_state("word-state", k2)["count"] == 1
