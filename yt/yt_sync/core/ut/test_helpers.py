import uuid

import pytest

from yt.yt_sync.core.helpers import is_valid_collocation_id


@pytest.mark.parametrize("collocation_id", [None, "", "0-0-0-0"])
def test_bad_collocation_id(collocation_id):
    assert is_valid_collocation_id(collocation_id) is False


def test_good_collocation_id():
    assert is_valid_collocation_id(str(uuid.uuid4())) is True
