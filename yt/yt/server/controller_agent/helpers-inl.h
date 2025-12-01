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

        auto checkUserJobSpec = [] (const THashMap<std::string, NScheduler::TVolumePtr>& volumes) {
            THashSet<std::string> allVolumesMediums;
            for (const auto& [_, volume] : volumes) {
                if (!volume->DiskRequest) {
                    continue;
                }

                if (auto diskRequest = volume->DiskRequest->TryGetConcrete<NScheduler::TDiskRequestConfig>()) {
                    if (diskRequest->MediumName) {
                        allVolumesMediums.insert(*diskRequest->MediumName);
                    }
                }
            }

            // TODO(krasovav): Delete after supporting multiple mediums.
            if (allVolumesMediums.size() > 1) {
                THROW_ERROR_EXCEPTION("Disk requests with 2 or more different Mediums are not currently supported")
                    << TErrorAttribute("volumes", volumes);
            }

            // TODO(krasovav): Delete after supporting multiple diskrequests.
            if (CountOfNonTmpfsVolumes(volumes) > 1) {
                THROW_ERROR_EXCEPTION("Volume request with 2 or more different not tmpfs disk request are not currently supported")
                    << TErrorAttribute("volumes", volumes);
            }
        };

        if constexpr (std::is_same_v<TSpec, NScheduler::TVanillaTaskSpec>) {
            checkUserJobSpec(spec->Volumes);
        } else if constexpr (std::is_same_v<TSpec, NScheduler::TMapOperationSpec>) {
            checkUserJobSpec(spec->Mapper->Volumes);
        } else if constexpr (std::is_same_v<TSpec, NScheduler::TReduceOperationSpec>) {
            checkUserJobSpec(spec->Reducer->Volumes);
        } else if constexpr (std::is_same_v<TSpec, NScheduler::TMapReduceOperationSpec>) {
            checkUserJobSpec(spec->Reducer->Volumes);
            if (spec->Mapper) {
                checkUserJobSpec(spec->Mapper->Volumes);
            }
            if (spec->ReduceCombiner) {
                checkUserJobSpec(spec->ReduceCombiner->Volumes);
            }
        } else if constexpr (std::is_same_v<TSpec, NScheduler::TVanillaOperationSpec>) {
             for (const auto& [_, task] : spec->Tasks) {
                checkUserJobSpec(task->Volumes);
            }
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing operation spec") << ex;
    }
    return spec;
}

template <class TOptions>
TOptions CreateOperationOptions(const TOptions& options, const NYTree::INodePtr& patch)
{
    using NYTree::ConvertTo;

    if (!patch) {
        return options;
    }
    return ConvertTo<TOptions>(PatchNode(ConvertToNode(options), patch));
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
void TAvgSummary<T>::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Sum_);
    PHOENIX_REGISTER_FIELD(2, Count_);
    registrar.AfterLoad([] (TThis* this_, auto& /*context*/) {
        this_->Avg_ = this_->CalcAvg();
    });
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
