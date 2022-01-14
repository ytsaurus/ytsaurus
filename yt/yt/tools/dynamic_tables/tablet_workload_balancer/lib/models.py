from abc import ABCMeta, abstractmethod
from collections import defaultdict
from datetime import datetime, timedelta
import yt.wrapper as yt

from yt.test_helpers import wait, WaitFailed


class State:
    def __init__(self, ts, cell_to_tablets, node_to_cells):
        self.ts = ts
        self.cell_to_tablets = cell_to_tablets
        self.node_to_cells = node_to_cells


class ActionSetResult():
    def __init__(self, ok=False, text=None, states=None, error=None, unknown=False):
        self.text = text
        self.ok = ok
        self.states = states
        self.err = error
        self.unknown = unknown

    def __str__(self):
        return self.text if self.text is not None else ''

    def __bool__(self):
        return self.ok

    def throw(self):
        if self.err is not None:
            raise self.err


class ActionSet():
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    def execute(self):
        return self._execute()

    @abstractmethod
    def _execute(self):
        raise Exception('Abstract method has not yet been implemented.')

    @abstractmethod
    def __str__(self):
        raise Exception('Abstract method has not yet been implemented.')

    def format_time(self, hours=0, minutes=0):
        time = datetime.now() + timedelta(hours=hours, minutes=minutes)
        return time.strftime("%Y-%m-%d %H:%M:%SZ")


class MoveActionSet(ActionSet):
    def __init__(self):
        super(MoveActionSet, self).__init__()
        self.table_to_tablet_to_cell = defaultdict(dict)

    def add(self, tablet_id, cell_id, table):
        self.table_to_tablet_to_cell[table][tablet_id] = cell_id

    def __str__(self):
        return '\n'.join(["MoveAction(tablet_id={}, cell_id={}, table={})".format(
            tablet_id, cell_id, table)
            for table, tablet_to_cell in self.table_to_tablet_to_cell.items()
                for tablet_id, cell_id in tablet_to_cell.items()])

    def __nonzero__(self):
        return len(self.table_to_tablet_to_cell)

    def _get_state(self, action_id):
        return yt.get("#{}/@state".format(action_id))

    def _execute(self):
        table_to_action_id = dict()
        for table, tablet_to_cell in self.table_to_tablet_to_cell.items():
            try:
                action_id = yt.create("tablet_action", attributes={
                    "kind": "move",
                    "tablet_ids": list(tablet_to_cell.keys()),
                    "cell_ids": list(tablet_to_cell.values()),
                    "expiration_time": self.format_time(minutes=20)})
            except (yt.YtProxyUnavailable, yt.YtHttpResponseError) as err:
                return ActionSetResult(ok=False, text=str(err), error=err)
            table_to_action_id[table] = action_id

        try:
            wait(lambda: all(self._get_state(action_id) in ('completed', 'failed')
                 for action_id in table_to_action_id.values()))
        except (WaitFailed, yt.YtError):
            pass

        try:
            states = {table : self._get_state(action_id) for table, action_id in table_to_action_id.items()}
        except yt.YtError as err:
            return ActionSetResult(ok=False, text=str(err), error=err, unknown=True)
        return ActionSetResult(ok=all(state == "completed" for state in states.values()),
                               states=states)
