import operator

from .simple import Simple
from .utils import get_tablets_params
from .. import fields


class StaticGreedy(Simple):
    def _greedy_step(self, tablet_id_to_magnitude):
        size = len(self.state.cell_to_tablets)
        boxes = [set() for _ in range(size)]
        weights = [0] * size

        for tablet_id, value in sorted(tablet_id_to_magnitude.items(),
                                       key=operator.itemgetter(1), reverse=True):
            idx = weights.index(min(weights))
            weights[idx] += value
            boxes[idx].add(tablet_id)
        return boxes, weights

    def _make_arrangement(self, boxes, weights):
        arrangement = {}
        for cell_id, tablets in sorted(self.state.cell_to_tablets.items(),
                                       key=lambda x: len(x[1])):
            max_items_in_common = 0
            idx_max = 0
            for idx, box in enumerate(boxes):
                common = len(box.intersection(tablets))
                if common > max_items_in_common:
                    max_items_in_common = common
                    idx_max = idx

            arrangement[cell_id] = boxes[idx_max]
            boxes.pop(idx_max)
        return arrangement

    def _step(self):
        tablet_id_to_magnitude = get_tablets_params(
            self.snapshot[fields.TABLES_FIELD], self.opt_field)
        boxes, weights = self._greedy_step(tablet_id_to_magnitude)
        arrangement = self._make_arrangement(boxes, weights)
        self._apply_arrangement(arrangement)
