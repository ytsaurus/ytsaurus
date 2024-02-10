#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

template <class TSpec>
TIntrusivePtr<TSpec> ParseOperationSpec(NYTree::INodePtr specNode)
{
    auto spec = New<TSpec>();
    try {
        spec->Load(specNode);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing operation spec") << ex;
    }
    return spec;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TAvgSummary<T>::TAvgSummary()
    : TAvgSummary(T(), 0)
{ }

template <class T>
TAvgSummary<T>::TAvgSummary(T sum, i64 count)
    : Sum_(sum)
    , Count_(count)
    , Avg_(CalcAvg())
{ }

template <class T>
std::optional<T> TAvgSummary<T>::CalcAvg()
{
    return Count_ == 0 ? std::optional<T>() : Sum_ / Count_;
}

template <class T>
void TAvgSummary<T>::AddSample(T sample)
{
    Sum_ += sample;
    ++Count_;
    Avg_ = CalcAvg();
}

template <class T>
void TAvgSummary<T>::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Sum_);
    Persist(context, Count_);
    if (context.IsLoad()) {
        Avg_ = CalcAvg();
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TFuture<std::optional<T>> WithSoftTimeout(
    TFuture<T> future,
    TDuration timeout,
    TCallback<void(const TErrorOr<T>&)> onFinishedAfterTimeout)
{
    auto timeoutFuture = NConcurrency::TDelayedExecutor::MakeDelayed(timeout);

    return AnySet(
        /*futures*/ std::vector{future.template As<void>(), timeoutFuture},
        /*options*/ {/*PropagateCancelationToInput*/ false, /*CancelInputOnShortcut*/ false})
        .Apply(BIND([
                future = std::move(future),
                onFinishedAfterTimeout = std::move(onFinishedAfterTimeout)
            ] (const TError& combinedResultOrError) -> std::optional<T> {
                if (auto maybeResultOrError = future.TryGet()) {
                    if constexpr(std::is_same_v<T, void>) {
                        maybeResultOrError->ThrowOnError();
                        return true;
                    } else {
                        return maybeResultOrError->ValueOrThrow();
                    }
                }

                if (onFinishedAfterTimeout) {
                    future.Subscribe(std::move(onFinishedAfterTimeout));
                }

                // Handle very very unlikely errors from timeout future.
                YT_ASSERT(combinedResultOrError.IsOK());
                return std::nullopt;
            }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
