from .base import Balancing, WrongConfig
from .utils import (
    init_arrangement, get_tablets_params, sum_by, find_top_tablet_in_cell)
from .. import fields


class Simple(Balancing):
    def __init__(self, snapshot, cell_arrangements, node_arrangements, **kwargs):
        super(Simple, self).__init__(snapshot, cell_arrangements, node_arrangements)
        if fields.OPT_FIELD not in kwargs:
            raise WrongConfig('Expected {} for {} algorithm'.format(
                fields.OPT_FIELD, self.__class__.__name__))
        self.opt_field = kwargs.get(fields.OPT_FIELD)
        self.plot_field = kwargs.get(fields.PLOT_FIELD)

    def _step(self):
        tablet_id_to_magnitude = get_tablets_params(
            self.snapshot[fields.TABLES_FIELD], self.opt_field)
        cell_id_to_magnitude = sum_by(tablet_id_to_magnitude, self.state.cell_to_tablets)

        min_cell_id = min(cell_id_to_magnitude, key=cell_id_to_magnitude.get)
        max_cell_id = max(cell_id_to_magnitude, key=cell_id_to_magnitude.get)
        max_tablet_id = find_top_tablet_in_cell(
            max_cell_id, tablet_id_to_magnitude, self.state.cell_to_tablets, max)

        self.state.cell_to_tablets[max_cell_id].remove(max_tablet_id)
        self.state.cell_to_tablets[min_cell_id].add(max_tablet_id)
        self.actions.add(max_tablet_id, min_cell_id)

    def _processed_plot(self):
        cells = sum_by(
            get_tablets_params(self.snapshot[fields.TABLES_FIELD], self.plot_field
                               if self.plot_field is not None else self.opt_field),
            self.state.cell_to_tablets)

        self.processed_plot = self._make_plot_struct(self.snapshot[fields.TS_FIELD], cells)

    def _initial_plot(self):
        cells = sum_by(
            get_tablets_params(self.snapshot[fields.TABLES_FIELD], self.plot_field
                               if self.plot_field is not None else self.opt_field),
            init_arrangement(self.snapshot))

        self.initial_plot = self._make_plot_struct(self.snapshot[fields.TS_FIELD], cells)
