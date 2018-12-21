#pragma once
#include "public.h"

#include <yt/core/misc/optional.h>
#include <yt/core/profiling/timing.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A max-min balancing device.
/*!
 *  Balances several contenders in a way that aims to minimize their difference
 *  in weight. (It does so by maximizing the weight of the least weighty
 *  contender.) The weights are increased manually by the client of the class
 *  and are then periodically decayed (see ctor parameters).
 *
 *  Contenders are added via #AddContender() and are passed around by-value.
 *  This means that:
 *    1. contenders must be copyable, and copying them must be cheap;
 *    2. comparing two contenders via == must return true iff they're identical,
 *       i.e. represent one and the same contender. (Thus, #AddContender()
 *       shouldn't be called twice with equal arguments.)
 *  Indexes, pointers and iterators make ideal contenders.
 *
 *  The contender with the minimum weight is chosen via #TakeWinner().
 *  #TakeWinnerIf() may be used to find, among contenders matching given
 *  predicate, the one with the minimum weight.
 *
 *  To increase the contender's weight, use to #AddWeight().
 *
 *  Not thread safe.
 */
template <typename T, typename W>
class TDecayingMaxMinBalancer
{
public:
    TDecayingMaxMinBalancer(W decayFactor, TDuration decayInterval);

    void AddContender(T contender, W initialWeight = W());

    // Returns null if there're no contenders.
    std::optional<T> TakeWinner();

    // Returns null if there're no contenders or no contender satisfies #pred.
    template <typename P>
    std::optional<T> TakeWinnerIf(P pred);

    void AddWeight(T winner, W extraWeight);

    void ResetWeights();

private:
    struct TWeighedContender
    {
        T Contender;
        W Weight;

        TWeighedContender(T contender, W weight);

        bool operator<(const TWeighedContender& rhs) const { return Weight < rhs.Weight; }
    };

    void MaybeDecay();

    typename std::vector<TWeighedContender>::iterator FindContender(T contender);

    W DecayFactor_;
    NProfiling::TCpuDuration DecayInterval_;
    NProfiling::TCpuInstant NextDecayInstant_;

protected: // for testing
    std::vector<TWeighedContender> Contenders_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define MAX_MIN_BALANCER_INL_H_
#include "max_min_balancer-inl.h"
#undef MAX_MIN_BALANCER_INL_H_
