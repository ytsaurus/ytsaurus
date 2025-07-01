import yt.wrapper as yt


def test():
    yt.create("table", "//tmp/t", attributes={
        "schema": [{"name": "a", "type": "int64"}]
    })
    rows = [{"a": 42}]
    yt.write_table("//tmp/t", rows)

    q = yt.run_query("yql", "select a+1 as b from `//tmp/t`")

    assert q.get_state() == "completed"
    results = q.get_results()
    assert len(results) == 1
    assert list(results[0]) == [{"b": 43}]
