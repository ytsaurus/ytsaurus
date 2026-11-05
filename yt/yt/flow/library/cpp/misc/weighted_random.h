#pragma once

#include <type_traits>
#include <util/random/random.h>
#include <vector>

#include "public.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(IRandomDoubleProvider);

//! Encapsulates calls to random functions and provides common interface for them.
class IRandomDoubleProvider
    : public TRefCounted
{
public:
    ~IRandomDoubleProvider() override = default;
    virtual double Get() = 0;

    double Get(double min, double max);
};

DEFINE_REFCOUNTED_TYPE(IRandomDoubleProvider);

////////////////////////////////////////////////////////////////////////////////

class TDefaultRandomDoubleProvider
    : public IRandomDoubleProvider
{
public:
    double Get() override;
};

////////////////////////////////////////////////////////////////////////////////

//! Allows to make a random pick of any set of objects with the probabilities proportional to their weights.
template <class TKey>
class TWeightedRandom
{
public:
    template <typename TContainer>
    TWeightedRandom(TContainer&& container, const IRandomDoubleProviderPtr& randomDoubleProvider = New<TDefaultRandomDoubleProvider>());

    TKey operator()() const;

private:
    IRandomDoubleProviderPtr RandomDoubleProvider_;
    double Sum_ = 0;
    std::vector<std::pair<TKey, double>> WeightPrefSum_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define WEIGHTED_RANDOM_H_
#include "weighted_random-inl.h"
#undef WEIGHTED_RANDOM_H_
