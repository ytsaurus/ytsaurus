from .realtime import SumOfSquares
from .simple import Simple
from .static_greedy import StaticGreedy

ALGORITHMS = {
    'simple': Simple,
    'static-greedy': StaticGreedy,
    'sum-of-squares': SumOfSquares,
}
