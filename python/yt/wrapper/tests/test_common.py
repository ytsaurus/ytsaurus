from yt.wrapper.common import update

def test_update():
    assert update({"a": 10}, {"b": 20}) == {"a": 10, "b": 20}
    assert update({"a": 10}, {"a": 20, "b": {"c": 10}}) == {"a": 20, "b": {"c": 10}}
    assert update({"a": 10, "b": "some"}, {"a": 20, "b": {"c": 10}}) == {"a": 20, "b": {"c": 10}}


