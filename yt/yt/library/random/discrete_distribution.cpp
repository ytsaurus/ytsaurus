#include "discrete_distribution.h"

#include <util/random/random.h>

#include <algorithm>
#include <numeric>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

int DiscreteDistribution(std::vector<double> weights)
{
    double sum = std::accumulate(weights.cbegin(), weights.cend(), 0.0);
    for (auto& weight : weights) {
        weight /= sum;
    }

    std::partial_sum(weights.cbegin(), weights.cend(), weights.begin());
    return std::upper_bound(weights.begin(), weights.end(), RandomNumber<double>()) - weights.begin();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
