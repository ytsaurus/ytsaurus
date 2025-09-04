#pragma once
#include "public.h"

#include <yt/yt/core/profiling/timing.h>

#include <optional>

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
 *  The contender with the minimum weight is chosen via #ChooseWinner().
 *  #ChooseWinnerIf() may be used to find, among contenders matching given
 *  predicate, the one with the minimum weight.
 *
 *  If the initial value for all contenders is the neutral element for multiplication
 *  over the weight type, #ChooseRangeWinner can be used to choose among a range
 *  of potential contenders possibly not added to the balancer.
 *
 *  To increase the contender's weight, use #AddWeight() for elements known to exist
 *  and #AddWeightWithDefault() otherwise. Same restrictions on #defaultWeight apply to
 *  the latter method.
 *
 *  Not thread safe.
 */
template <typename T, typename W>
class TDecayingMaxMinBalancer
{
public:
    TDecayingMaxMinBalancer(W decayFactor, TDuration decayInterval);

    void AddContender(T contender, W initialWeight = W());

    //! Selects winner between added contenders.
    //! Returns null if there aren't any contenders.
    //! Complexity: linear in the number of stored contenders.
    std::optional<T> ChooseWinner();

    //! Selects winner between added contenders that satisfy #pred.
    //! Returns null if there are no contenders or no contender satisfies #pred.
    //! Complexity: linear in the number of stored contenders.
    template <typename P>
    std::optional<T> ChooseWinnerIf(P pred);

    //! Selects winner between potential contenders in the provided range.
    //! If a contender was not added to the balancer, #defaultWeight will be used as its weight.
    //! Complexity: linear in the size of the range.
    //! NB: Results may be unexpected if #defaultWeight is not equal to both the initial weight
    //! for all previously added contenders, as well as the neutral element for the multiplication
    //! operation over the weight type. This holds true for most usecases, allowing to populate
    //! contenders lazily.
    template <typename R>
    std::optional<T> ChooseRangeWinner(R range, W defaultWeight = W());

    //! Increase weight of existing contender.
    void AddWeight(T winner, W extraWeight);
    //! If contender is not present, it is initialized with #defaultWeight before
    //! performing the requested increment.
    //! NB: Same note about #defaultWeight applies as for #ChooseRangeWinner().
    void AddWeightWithDefault(T winner, W extraWeight, W defaultWeight = W());

    void ResetWeights();

private:
    void MaybeDecay();

    W DecayFactor_;
    NProfiling::TCpuDuration DecayInterval_;
    NProfiling::TCpuInstant NextDecayInstant_;

protected: // for testing
    THashMap<T, W> ContenderToWeight_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define MAX_MIN_BALANCER_INL_H_
#include "max_min_balancer-inl.h"
#undef MAX_MIN_BALANCER_INL_H_
