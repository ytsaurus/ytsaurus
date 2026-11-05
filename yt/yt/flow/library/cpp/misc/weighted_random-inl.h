#pragma once

#ifndef WEIGHTED_RANDOM_H_
    #error "Direct inclusion of this file is not allowed, include versioned_value.h"
    // For the sake of sane code completion.
    #include "weighted_random.h"
#endif

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class TKey>
template <class TContainer>
TWeightedRandom<TKey>::TWeightedRandom(TContainer&& container, const IRandomDoubleProviderPtr& randomDoubleProvider)
    : RandomDoubleProvider_(randomDoubleProvider)
{
    THROW_ERROR_EXCEPTION_IF(container.empty(), "Container is empty");

    for (const auto& [key, weight] : container) {
        double weightConverted = static_cast<double>(weight);
        THROW_ERROR_EXCEPTION_IF(weightConverted < 0, "Can't have negative weight");
        Sum_ += weightConverted;
        WeightPrefSum_.push_back({key, Sum_});
    }

    THROW_ERROR_EXCEPTION_IF(Sum_ <= 0, "Sum of weights must be positive");
}

template <class TKey>
TKey TWeightedRandom<TKey>::operator()() const
{
    auto iter = std::ranges::lower_bound(WeightPrefSum_, RandomDoubleProvider_->Get(0, Sum_), {}, [] (const auto& p) {
        return p.second;
    });
    if (iter == WeightPrefSum_.end()) {
        return WeightPrefSum_.back().first;
    }
    return iter->first;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
