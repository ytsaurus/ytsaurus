from yt_queue_agent_test_base import TestQueueAgentBase

from yt_commands import (authors, create, get, wait_for_tablet_state)

from yt_type_helpers import normalize_schema, make_schema

import yt.environment.init_queue_agent_state as init_queue_agent_state

##################################################################


class TestCreateProducer(TestQueueAgentBase):
    @authors("apachee")
    def test_create_producer(self):
        create("producer", "//tmp/p")

        assert get("//tmp/p/@type") == "table"
        assert get("//tmp/p/@dynamic")

        expected_schema = make_schema(
            init_queue_agent_state.PRODUCER_OBJECT_TABLE_SCHEMA,
            strict=True,
            unique_keys=True,
        )
        assert normalize_schema(get("//tmp/p/@schema")) == expected_schema

        wait_for_tablet_state("//tmp/p", "mounted")
