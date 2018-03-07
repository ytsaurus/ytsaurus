#pragma once

#ifndef UPDATE_EXECUTOR_INL_H_
#error "Direct inclusion of this file is not allowed, include update_executor.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TUpdateParameters>
TUpdateExecutor<TKey, TUpdateParameters>::TUpdateExecutor(
    IInvokerPtr invoker,
    TCallback<TCallback<TFuture<void>()>(const TKey&, TUpdateParameters*)> createUpdateAction,
    TCallback<bool(const TUpdateParameters*)> shouldRemoveUpdateAction,
    TCallback<void(const TError&)> onUpdateFailed,
    TDuration period,
    const NLogging::TLogger& logger)
    : CreateUpdateAction_(createUpdateAction)
    , ShouldRemoveUpdateAction_(shouldRemoveUpdateAction)
    , OnUpdateFailed_(onUpdateFailed)
    , Invoker_(std::move(invoker))
    , Logger(logger)
    , UpdateExecutor_(New<NConcurrency::TPeriodicExecutor>(
        Invoker_,
        BIND(&TUpdateExecutor<TKey, TUpdateParameters>::ExecuteUpdates, MakeWeak(this)),
        period,
        NConcurrency::EPeriodicExecutorMode::Automatic))
{ }

template <class TKey, class TUpdateParameters>
void TUpdateExecutor<TKey, TUpdateParameters>::Start()
{
    UpdateExecutor_->Start();
}

template <class TKey, class TUpdateParameters>
void TUpdateExecutor<TKey, TUpdateParameters>::Stop()
{
    UpdateExecutor_->Stop();
}

template <class TKey, class TUpdateParameters>
void TUpdateExecutor<TKey, TUpdateParameters>::SetPeriod(TDuration period)
{
    UpdateExecutor_->SetPeriod(period);
}

template <class TKey, class TUpdateParameters>
TUpdateParameters* TUpdateExecutor<TKey, TUpdateParameters>::AddUpdate(const TKey& key, const TUpdateParameters& parameters)
{
    VERIFY_THREAD_AFFINITY(UpdateThread);

    auto pair = Updates_.insert(std::make_pair(key, TUpdateRecord(key, parameters)));
    YCHECK(pair.second);
    LOG_DEBUG("Item added to periodic updates (Key: %v)", key);
    return &pair.first->second.UpdateParameters;
}

template <class TKey, class TUpdateParameters>
void TUpdateExecutor<TKey, TUpdateParameters>::RemoveUpdate(const TKey& key)
{
    VERIFY_THREAD_AFFINITY(UpdateThread);

    YCHECK(Updates_.erase(key) == 1);
    LOG_DEBUG("Item removed from periodic updates (Key: %v)", key);
}

template <class TKey, class TUpdateParameters>
TUpdateParameters* TUpdateExecutor<TKey, TUpdateParameters>::FindUpdate(const TKey& key)
{
    VERIFY_THREAD_AFFINITY(UpdateThread);

    auto* result = FindUpdateRecord(key);
    return result ? &result->UpdateParameters : nullptr;
}

template <class TKey, class TUpdateParameters>
TUpdateParameters* TUpdateExecutor<TKey, TUpdateParameters>::GetUpdate(const TKey& key)
{
    VERIFY_THREAD_AFFINITY(UpdateThread);

    auto* result = FindUpdate(key);
    YCHECK(result);
    return result;
}

template <class TKey, class TUpdateParameters>
void TUpdateExecutor<TKey, TUpdateParameters>::Clear()
{
    VERIFY_THREAD_AFFINITY(UpdateThread);

    Updates_.clear();
}

template <class TKey, class TUpdateParameters>
void TUpdateExecutor<TKey, TUpdateParameters>::ExecuteUpdates()
{
    VERIFY_THREAD_AFFINITY(UpdateThread);

    LOG_INFO("Updating items (Count: %v)", Updates_.size());

    std::vector<TKey> updatesToRemove;
    std::vector<TFuture<void>> asyncResults;
    std::vector<TKey> requestKeys;
    for (auto& pair : Updates_) {
        const auto& key = pair.first;
        auto& updateRecord = pair.second;
        if (ShouldRemoveUpdateAction_(&updateRecord.UpdateParameters)) {
            updatesToRemove.push_back(key);
        } else {
            LOG_DEBUG("Updating item (Key: %v)", key);
            requestKeys.push_back(key);
            asyncResults.push_back(DoExecuteUpdate(&updateRecord));
        }
    }

    // Cleanup.
    for (auto key : updatesToRemove) {
        RemoveUpdate(key);
    }

    auto result = NConcurrency::WaitFor(CombineAll(asyncResults));
    YCHECK(result.IsOK());
    if (!result.IsOK()) {
        OnUpdateFailed_(result);
        return;
    }

    LOG_INFO("Update completed");
}

template <class TKey, class TUpdateParameters>
TFuture<void> TUpdateExecutor<TKey, TUpdateParameters>::ExecuteUpdate(const TKey& key)
{
    VERIFY_THREAD_AFFINITY(UpdateThread);

    auto* updateRecord = FindUpdateRecord(key);
    if (!updateRecord) {
        return VoidFuture;
    }
    return DoExecuteUpdate(updateRecord);
}

template <class TKey, class TUpdateParameters>
typename TUpdateExecutor<TKey, TUpdateParameters>::TUpdateRecord* TUpdateExecutor<TKey, TUpdateParameters>::FindUpdateRecord(const TKey& key)
{
    VERIFY_THREAD_AFFINITY(UpdateThread);

    auto it = Updates_.find(key);
    return it == Updates_.end() ? nullptr : &it->second;
}

template <class TKey, class TUpdateParameters>
TCallback<TFuture<void>()> TUpdateExecutor<TKey, TUpdateParameters>::CreateUpdateAction(const TKey& key, TUpdateParameters* updateParameters)
{
    auto updateAction = CreateUpdateAction_(key, updateParameters);
    if (!updateAction) {
        return {};
    }

    return BIND([key, this, updateAction = std::move(updateAction), this_ = MakeStrong(this)] () {
            return updateAction().Apply(
                BIND([=, this_ = MakeStrong(this)] (const TError& error) {
                    if (!error.IsOK()) {
                        OnUpdateFailed_(TError("Update of item failed (Key: %v)", key) << error);
                    }
                })
                .AsyncVia(Invoker_)
            );
        })
        .AsyncVia(Invoker_);
}

template <class TKey, class TUpdateParameters>
TFuture<void> TUpdateExecutor<TKey, TUpdateParameters>::DoExecuteUpdate(TUpdateRecord* updateRecord)
{
    VERIFY_THREAD_AFFINITY(UpdateThread);

    auto callback = CreateUpdateAction(updateRecord->Key, &updateRecord->UpdateParameters);
    if (!callback) {
        return updateRecord->LastUpdateFuture;
    }
    updateRecord->LastUpdateFuture = updateRecord->LastUpdateFuture.Apply(callback);
    return updateRecord->LastUpdateFuture;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
