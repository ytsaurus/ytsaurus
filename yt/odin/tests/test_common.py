from yt_odin.odinserver.common import get_part

import pytest


def test_get_part():
    sorted_clusters = [
        "alfa", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliet", "kilo",
        "lima", "mike", "november", "oscar", "papa", "quebec", "romeo", "sierra", "tango", "uniform", "victor",
        "whiskey", "x-ray", "yankee", "zulu"]
    clusters = sorted_clusters[::]
    import random
    random.shuffle(clusters)
    assert len(clusters) == 26  # one word for one alphabet letter
    assert get_part(clusters, 0, 1) == sorted_clusters
    with pytest.raises(AssertionError):
        get_part(clusters, 1, 1)
    with pytest.raises(AssertionError):
        get_part(clusters, -1, 1)
    with pytest.raises(AssertionError):
        get_part(clusters, 0, 0)
    assert get_part(clusters, 1, 2) == sorted_clusters[13:]
    assert get_part(clusters, 1, 8) == ["echo", "foxtrot", "golf", "hotel"]
