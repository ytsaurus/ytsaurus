from collections import Counter
from enum import Enum
import logging
from typing import Any

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtTable

from .base import ActionBase

LOG = logging.getLogger("yt_sync")


class _State(Enum):
    START = -1
    INIT = 0
    DONE = 1
    CHUNK_LIST_ID = 2
    TABLET_CHUNK_LIST_IDS = 3
    GET_IDS_LIST = 4
    GET_IDS = 5
    PROCESS_IDS = 6
    FORCE_CHUNK_COMPACTION = 7
    REMOUNT = 8
    FINISH_REMOUNT = 9


class EliminateChunkViews(ActionBase):
    def __init__(self, table: YtTable):
        assert table.table_type == YtTable.Type.TABLE, "Only basic tables are supported"
        assert not table.is_ordered, "Queues are not supported"
        super().__init__()
        self._table: YtTable = table
        self._state = _State.START
        self._result: Any | None = None
        self._chunk_list_id: int | None = None
        self._tablet_chunk_list_ids: list[int] | None = None
        self._results: list[Any] = []
        self._eliminating = False

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        assert self._table.exists, "Table is expected to exist"
        match self._state:
            case _State.DONE:
                return False
            case _State.INIT | _State.START:
                self._result = batch_client.get(self._table.path + "/@chunk_list_id")
                self._state = _State.CHUNK_LIST_ID
                return True
            case _State.TABLET_CHUNK_LIST_IDS:
                self._result = batch_client.get(f"#{self._chunk_list_id}/@child_ids")
                self._state = _State.GET_IDS_LIST
                return True
            case _State.GET_IDS:
                assert self._tablet_chunk_list_ids
                for c in self._tablet_chunk_list_ids:
                    self._results.append(batch_client.get(f"#{c}/@child_ids"))
                self._state = _State.PROCESS_IDS
                return True
            case _State.FORCE_CHUNK_COMPACTION:
                self._result = batch_client.set(self._table.path + "/@forced_chunk_view_compaction_revision", 1)
                return True
            case _State.REMOUNT:
                self._result = batch_client.remount_table(self._table.path)
                return True
            case _State.FINISH_REMOUNT:
                self._result = batch_client.remove(self._table.path + "/@forced_chunk_view_compaction_revision")
                return True
            case _:
                assert False, f"Unexpected state: {self._state}."

    @property
    def is_awaiting(self) -> bool:
        return True

    def process(self):
        match self._state:
            case _State.DONE | _State.INIT:
                pass
            case _State.FORCE_CHUNK_COMPACTION:
                self.assert_response(self._result)
                self._result = None
                self._state = _State.REMOUNT
            case _State.REMOUNT:
                self.assert_response(self._result)
                self._result = None
                self._state = _State.FINISH_REMOUNT
            case _State.FINISH_REMOUNT:
                self.assert_response(self._result)
                self._result = None
                self._state = _State.INIT
            case _State.CHUNK_LIST_ID:
                self.assert_response(self._result)
                self._chunk_list_id = self._result.get_result()  # type: ignore
                self._result = None
                self._state = _State.TABLET_CHUNK_LIST_IDS
            case _State.GET_IDS_LIST:
                self.assert_response(self._result)
                self._tablet_chunk_list_ids = self._result.get_result()  # type: ignore
                self._result = None
                self._state = _State.GET_IDS
            case _State.PROCESS_IDS:
                counter = Counter()
                for result in self._results:
                    self.assert_response(result)
                    for child_id in result.get_result():
                        type = child_id.split("-")[2][-4:].lstrip("0")
                        counter[type] += 1
                self._results = []
                if self._eliminating:
                    chunk_view_count = counter.get("7b", 0)
                    if chunk_view_count == 0:
                        LOG.warning(
                            "%s:%s: chunk views successfully eliminated.",
                            self._table.cluster_name,
                            self._table.path,
                        )
                        self._state = _State.DONE
                        return
                    LOG.info(
                        "%s:%s: %s chunk views remaining.",
                        self._table.cluster_name,
                        self._table.path,
                        chunk_view_count,
                    )
                    self._state = _State.INIT
                    return
                if counter.get("65", 0) > 0:  # chunk list
                    raise Exception(
                        "Source replica table has nested tablet chunks lists, probably it was "
                        "bulk inserted into. Cannot do versioned remote copy."
                    )
                if counter.get("7b", 0) > 0:  # chunk view
                    LOG.warning(
                        "%s:%s: eliminating %s chunk views.",
                        self._table.cluster_name,
                        self._table.path,
                        counter["7b"],
                    )
                    self._eliminating = True
                    self._state = _State.FORCE_CHUNK_COMPACTION
                    return
                LOG.info("%s:%s doesn't have chunk views, skipping...", self._table.cluster_name, self._table.path)
                self._state = _State.DONE
            case _:
                assert False, f"Unexpected state: {self._state}"
