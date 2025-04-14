from .conftest import authors
from .helpers import set_config_option

from yt.wrapper.operation_commands import add_failed_operation_stderrs_to_error_message

import yt.wrapper as yt

import pytest
import string
import random


@pytest.mark.usefixtures("yt_env_with_authentication")
class TestAuthCommands(object):
    @authors("pavel-bash")
    @add_failed_operation_stderrs_to_error_message
    def test_password_strength_validation(self):
        password = ''.join(random.choice(string.ascii_letters) for i in range(11))
        yt.set_user_password("admin", password)
        with set_config_option("enable_password_strength_validation", True):
            with pytest.raises(ValueError):
                yt.set_user_password("admin", password)

        password = ''.join(random.choice(string.ascii_letters) for i in range(12))
        yt.set_user_password("admin", password)

        password = ''.join(random.choice(string.ascii_letters) for i in range(129))
        yt.set_user_password("admin", password)
        with set_config_option("enable_password_strength_validation", True):
            with pytest.raises(ValueError):
                yt.set_user_password("admin", password)
