#pragma once

#include "public.h"

namespace NYT::NLsm::NTesting {

////////////////////////////////////////////////////////////////////////////////

class TOptimizer
{
public:
    struct TParameter
    {
        double Min = 0.0;
        double Max = 1.0;
        double Weight = 1;
        bool IsInteger = false;
    };

    using TFunction = std::function<double(const std::vector<double>&)>;

    TOptimizer(std::vector<TParameter> parameters)
        : Parameters_(std::move(parameters))
    { }

    std::vector<double> Optimize(
        TFunction function,
        int numIters = 30,
        int numDirections = 5,
        float initialStep = 0.2,
        float stepMultiple = 0.9);

private:
    std::vector<TParameter> Parameters_;
    std::vector<double> NormalizedValues_;
    TFunction Function_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
