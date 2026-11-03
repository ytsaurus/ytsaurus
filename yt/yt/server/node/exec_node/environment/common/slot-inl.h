#ifndef SLOT_INL_H_
#error "Direct inclusion of this file is not allowed, include slot.h"
// For the sake of sane code completion.
#include "slot.h"
#endif

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

template <class TName, CCallableReturningFuture TCallback>
auto TUserSlotBase::RunPreparationAction(const TName& actionName, bool uncancelable, const TCallback& action) -> decltype(action())
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    using TTypedFuture = decltype(action());
    using TUnderlyingReturnType = typename TFutureTraits<TTypedFuture>::TUnderlying;

    if (PreparationCanceled_) {
        YT_TLOG_DEBUG("Skip preparation action since preparation is canceled")
            .With("ActionName", actionName);

        return MakeFuture<TUnderlyingReturnType>(TError("Job preparation canceled")
            .With("slot_index", SlotIndex_));
    } else {
        YT_TLOG_DEBUG("Running preparation action")
            .With("ActionName", actionName);

        auto future = action();

        return uncancelable
            ? future.ToUncancelable()
            : future;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
