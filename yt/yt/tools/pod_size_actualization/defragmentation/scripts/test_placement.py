import unittest

import pytest

from yt.yt.tools.pod_size_actualization.defragmentation.scripts.shared import trim_proportional_placement


@pytest.mark.authors(["tea-mur", "wolkozaya"])
class TrimPlacementTest(unittest.TestCase):
    def test_preserves_required_pods_and_numa_assignments(self):
        extras = {'a': 2, 'b': 5}
        placement = {
            'h1': {'a': [0, 1, None], 'b': [1, 0]},
            'h2': {'b': [None, 1, 0]},
            'h3': {'b': [0]},
        }
        trim_proportional_placement({'a': 1, 'b': 1}, {'a': 1, 'b': 1}, extras, placement)
        self.assertEqual(extras, {'a': 2, 'b': 2})
        self.assertEqual(placement, {
            'h1': {'a': [0, 1, None], 'b': [1, 0]},
            'h2': {'b': [None]},
        })

    def test_rounding_zero_weights_and_empty_result(self):
        for weights, counts, expected in [
            ({'a': 2, 'b': 3}, (1, 5), (1, 2)),
            ({'a': 0.1, 'b': 0.3}, (1, 5), (1, 3)),
            ({'a': 1, 'b': 1}, (0, 5), (0, 0)),
            ({'a': 1, 'b': 0}, (2, 0), (2, 0)),
            ({}, (0, 0), (0, 0)),
        ]:
            with self.subTest(weights=weights, counts=counts):
                extras = dict(zip(('a', 'b'), counts))
                placement = {'h': {c: [None] * k for c, k in extras.items()}}
                trim_proportional_placement({}, weights, extras, placement)
                self.assertEqual(extras, dict(zip(('a', 'b'), expected)))
                self.assertEqual(placement, {'h': {
                    c: [None] * k for c, k in extras.items() if k
                }} if any(expected) else {})
                trim_proportional_placement({}, weights, extras, placement)
                self.assertEqual(extras, dict(zip(('a', 'b'), expected)))


if __name__ == '__main__':
    unittest.main()
