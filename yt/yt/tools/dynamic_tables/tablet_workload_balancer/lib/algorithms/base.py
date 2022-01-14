from abc import ABCMeta, abstractmethod

from .. import fields, models


class WrongConfig(Exception):
    pass


class Balancing():
    __metaclass__ = ABCMeta

    def __init__(self, snapshot, cell_to_tablets, node_to_cells):
        self.snapshot = snapshot

        for tablets in snapshot[fields.TABLES_FIELD].values():
            for tablet in tablets:
                if tablet[fields.CELL_ID_FIELD] not in cell_to_tablets.keys():
                    raise Exception("Some cells from statistics have unknows addresses.")

        self.state = models.State(snapshot[fields.TS_FIELD], cell_to_tablets, node_to_cells)
        self.final = False
        self.processed_plot = None
        self.initial_plot = None
        self.actions = models.MoveActionSet()

    def process(self):
        if not self.final:
            self._step()
            self._processed_plot()
            self._initial_plot()
            self.final = True
        return self.actions, self.state, self.processed_plot, self.initial_plot

    @abstractmethod
    def _processed_plot(self):
        raise Exception('Abstract method has not yet been implemented.')

    @abstractmethod
    def _initial_plot(self):
        raise Exception('Abstract method has not yet been implemented.')

    @abstractmethod
    def _step(self):
        raise Exception('Abstract method has not yet been implemented.')

    def _make_plot_struct(self, ts, cells):
        return {
            fields.TS_FIELD : ts,
            fields.CELLS_FIELD : cells,
        }

    def _apply_arrangement(self, arrangement):
        tablet_id_to_table = {
            tablet_info[fields.TABLET_ID_FIELD] : table
                for table, tablets in self.snapshot[fields.TABLES_FIELD].items()
                for tablet_info in tablets
        }
        for cell_id in arrangement:
            for tablet_id in arrangement[cell_id] - self.state.cell_to_tablets[cell_id]:
                self.actions.add(tablet_id, cell_id, tablet_id_to_table[tablet_id])

        self.state.cell_to_tablets = arrangement
