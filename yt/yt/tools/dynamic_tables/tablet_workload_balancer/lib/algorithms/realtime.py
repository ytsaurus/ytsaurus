from abc import abstractmethod
from enum import Enum

from .base import Balancing, WrongConfig
from .static_greedy import StaticGreedy
from .utils import get_tablets_params, sum_by, invert_dict
from .. import fields


VARIANCE_OF_MAX = 'variance_of_max'


class ActionType(Enum):
    swap = 1
    one_move = 2


class RealTimeBase(Balancing):
    def __init__(self, snapshot, cell_arrangements, node_arrangements, limits, **kwargs):
        super(RealTimeBase, self).__init__(snapshot, cell_arrangements, node_arrangements)

        self.limits = limits
        self.baseline_solver = self._init_baseline_solver()

    @abstractmethod
    def _should_trigger(self, current, optimal):
        raise Exception('Abstract method has not yet been implemented.')

    @abstractmethod
    def _init_baseline_solver(self):
        raise Exception('Abstract method has not yet been implemented.')

    @abstractmethod
    def _process(self):
        raise Exception('Abstract method has not yet been implemented.')

    def _step(self):
        self.tablet_id_to_magnitude = get_tablets_params(
            self.snapshot[fields.TABLES_FIELD], self.opt_field)

        _, cell_weights = self.baseline_solver._greedy_step(self.tablet_id_to_magnitude)
        self.cell_id_to_magnitude = sum_by(self.tablet_id_to_magnitude, self.state.cell_to_tablets)

        if not cell_weights or not self._should_trigger(cell_weights,
                                                        list(self.cell_id_to_magnitude.values())):
            return

        arrangement = self._process()
        self._apply_arrangement(arrangement)


class StatelessIterative(RealTimeBase):
    def __init__(self, snapshot, cell_arrangements, node_arrangements, **kwargs):
        super(StatelessIterative, self).__init__(snapshot, cell_arrangements, node_arrangements, **kwargs)

        self.baseline_solver = StaticGreedy(snapshot, cell_arrangements, node_arrangements, **kwargs)
        self.opt_field = self.baseline_solver.opt_field

    def _should_trigger(self, optimal, current):
        if VARIANCE_OF_MAX in self.limits:
            return max(current) > (1 + self.limits[VARIANCE_OF_MAX]) * max(optimal)
        raise WrongConfig('expected {} field in {}'.format(VARIANCE_OF_MAX, 'limits'))

    def _init_baseline_solver(self):
        pass

    def _initial_plot(self):
        self.baseline_solver._initial_plot()
        self.initial_plot = self.baseline_solver.initial_plot

    def _processed_plot(self):
        self.baseline_solver.state.cell_to_tablets = self.state.cell_to_tablets
        self.baseline_solver._processed_plot()
        self.processed_plot = self.baseline_solver.processed_plot

    @abstractmethod
    def metric(self):
        raise Exception('Abstract method has not yet been implemented.')

    def _get_action_cost(self, action_type):
        if action_type is ActionType.swap:
            return 2
        elif action_type is ActionType.one_move:
            return 1
        return 0

    @abstractmethod
    def recalculate_metric_move(self, metric, tablet_id, cell_id):
        raise Exception('Abstract method has not yet been implemented.')

    def recalculate_metric_swap(self, metric, first_tablet_id, second_tablet_id):
        raise Exception('Abstract method has not yet been implemented.')

    def _move_tablet(self, tablet_id, cell_id):
        old_cell_id = self.tablet_to_cell[tablet_id]
        self.tablet_to_cell[tablet_id] = cell_id
        self.cell_id_to_magnitude[cell_id] += self.tablet_id_to_magnitude[tablet_id]
        self.cell_id_to_magnitude[old_cell_id] -= self.tablet_id_to_magnitude[tablet_id]

    def _process(self):
        self.tablet_to_cell = invert_dict(self.state.cell_to_tablets)

        actions_left = self.limits.get('move_action_count', len(self.tablet_id_to_magnitude) ** 2)
        current_metric = self.metric()
        while actions_left > 0:
            action_type = None
            action_args = None
            min_metric = current_metric

            for first in self.tablet_id_to_magnitude.keys():
                for cell_id in self.cell_id_to_magnitude.keys():
                    metric = self.recalculate_metric_move(current_metric, first, cell_id)
                    if min_metric > metric:
                        min_metric = metric
                        action_type = ActionType.one_move
                        action_args = (first, cell_id)

                if actions_left < 2:
                    continue

                for second in self.tablet_id_to_magnitude.keys():
                    if second >= first:
                        continue

                    if self.tablet_to_cell[first] == self.tablet_to_cell[second]:
                        continue

                    metric = self.recalculate_metric_swap(current_metric, first, second)
                    if min_metric > metric:
                        min_metric = metric
                        action_type = ActionType.swap
                        action_args = (first, second)

            if action_type is None:
                break

            actions_left -= self._get_action_cost(action_type)
            if action_type is ActionType.one_move:
                tablet_id, cell_id = action_args
                self._move_tablet(tablet_id, cell_id)
            elif action_type is ActionType.swap:
                first, second = action_args
                first_cell_id = self.tablet_to_cell[first]
                second_cell_id = self.tablet_to_cell[second]
                self._move_tablet(first, second_cell_id)
                self._move_tablet(second, first_cell_id)
            current_metric = min_metric

        return invert_dict(self.tablet_to_cell)


class SumOfSquares(StatelessIterative):
    def __init__(self, snapshot, cell_arrangements, node_arrangements, **kwargs):
        super(SumOfSquares, self).__init__(snapshot, cell_arrangements, node_arrangements, **kwargs)

    def metric(self):
        return sum(map(lambda x: x*x, self.cell_id_to_magnitude.values()))

    def recalculate_metric_move(self, metric, tablet_id, cell_id):
        cell_from = self.cell_id_to_magnitude[self.tablet_to_cell[tablet_id]]
        cell_to = self.cell_id_to_magnitude[cell_id]
        tablet = self.tablet_id_to_magnitude[tablet_id]

        metric -= cell_from ** 2 - (cell_from - tablet) ** 2
        metric += (cell_to + tablet) ** 2 - cell_to ** 2
        return metric

    def recalculate_metric_swap(self, metric, first_tablet_id, second_tablet_id):
        cell1 = self.cell_id_to_magnitude[self.tablet_to_cell[first_tablet_id]]
        cell2 = self.cell_id_to_magnitude[self.tablet_to_cell[second_tablet_id]]

        tablet1 = self.tablet_id_to_magnitude[first_tablet_id]
        tablet2 = self.tablet_id_to_magnitude[second_tablet_id]

        metric -= cell1 ** 2 + cell2 ** 2
        metric += (cell1 - tablet1 + tablet2) ** 2 + (cell2 + tablet1 - tablet2) ** 2
        return metric
