#ifndef COMPACTION_HINT_FETCHING_INL_H_
#error "Direct inclusion of this file is not allowed, include compaction_hint_fetching.h"
// For the sake of sane code completion.
#include "compaction_hint_fetching.h"
#endif

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

template <template <class T> class TFutureType, class T, class THandler>
void TCompactionHintFetchPipeline::SubscribeWithErrorHandling(const TFutureType<T>& future, THandler&& handler)
{
    future.Subscribe(BIND(
        &TCompactionHintFetchPipeline::WrapWithErrorHandling<
            T,
            THandler,
            std::conditional_t<std::is_same_v<TFutureType<T>, TUniqueFuture<T>>, TErrorOr<T>&&, const TErrorOr<T>&>
        >,
        MakeWeak(this),
        Passed(std::forward<THandler>(handler)))
        .Via(GetEpochAutomatonInvoker()));
}

template <class T, class THandler, class TErrorOrTType>
void TCompactionHintFetchPipeline::WrapWithErrorHandling(THandler&& handler, TErrorOrTType&& errorOrRsp)
{
    if (!errorOrRsp.IsOK()) {
        OnRequestFailed(errorOrRsp);
        return;
    }

    if constexpr (std::is_invocable_v<THandler>) {
        std::forward<THandler>(handler)();
    } else {
        std::forward<THandler>(handler)(
            std::forward<TErrorOrTType>(errorOrRsp).Value());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
