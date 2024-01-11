#include "optimizer.h"

#include <random>

namespace NYT::NLsm::NTesting {

////////////////////////////////////////////////////////////////////////////////

std::vector<double> TOptimizer::Optimize(
    TFunction function,
    int numIters,
    int numDirections,
    float initialStep,
    float stepMultiple)
{
    Function_ = std::move(function);
    NormalizedValues_.assign(ssize(Parameters_), 0.5);

    double step = initialStep;

    auto transformToOriginal = [&] (std::vector<double> pts) {
        for (int index = 0; index < ssize(Parameters_); ++index) {
            pts[index] = std::lerp(
                Parameters_[index].Min,
                Parameters_[index].Max,
                pts[index]);
        }
        return pts;
    };

    auto eval = [&] (std::vector<double> pts) {
        return function(transformToOriginal(pts));
    };

    std::mt19937 engine(std::random_device{}());
    std::normal_distribution<double> distribution;

    double currentOptimum = eval(NormalizedValues_);

#if 1

    for (double val = 0; val <= 1; val += initialStep) {
        auto value = eval({val});
        Cerr << "a.append(" << val << "); b.append(" << value << "); # PYTHON\n";
    }
    return {0.5};

#endif

    for (int iter = 0; iter < numIters; ++iter)
    {
        auto best = std::tuple(currentOptimum, NormalizedValues_);

        for (int guess = 0; guess < numDirections; ++guess) {
            std::vector<double> coordinates(ssize(Parameters_));
            double sumSquared = 0;
            for (int index = 0; index < ssize(Parameters_); ++index) {
                coordinates[index] = distribution(engine) * Parameters_[index].Weight;
                sumSquared += coordinates[index] * coordinates[index];
            }
            auto multiplier = step / std::sqrt(sumSquared);
            for (auto& coordinate : coordinates) {
                coordinate *= multiplier;
            }

            auto points = NormalizedValues_;
            for (int index = 0; index < ssize(Parameters_); ++index) {
                points[index] = std::clamp(
                    points[index] + coordinates[index],
                    0.0,
                    1.0);
            }
            auto value = eval(points);
            Cerr << Format("Eval (Points: %v, Value: %v)",
                MakeFormattableView(points, TDefaultFormatter{}),
                value) << Endl;
            Cerr << "a.append(" << points[0] << "); b.append(" << value << "); # PYTHON\n";
            best = std::min(best, std::tuple(value, points));
        }

        if (std::get<0>(best) < currentOptimum) {
            std::tie(currentOptimum, NormalizedValues_) = best;
            Cerr << "Updated optimum to " << currentOptimum << Endl;
        }

        step *= stepMultiple;

        Cerr << "Step = " << step << Endl;
    }

    return transformToOriginal(NormalizedValues_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
