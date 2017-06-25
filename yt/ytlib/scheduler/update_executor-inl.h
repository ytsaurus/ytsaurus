#ifndef UPDATE_EXECUTOR_INL_H_
#error "Direct inclusion of this file is not allowed, include update_executor.h"
#endif

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TUpdateParameters>
TUpdateExecutor<TKey, TUpdateParameters>::TUpdateExecutor(
    TCallback<TCallback<TFuture<void>()>(const TKey&, TUpdateParameters*)> createUpdateAction,
    TCallback<bool(const TUpdateParameters*)> shouldRemoveUpdateAction,
    NLogging::TLogger logger)
    : CreateUpdateAction_(createUpdateAction)
    , ShouldRemoveUpdateAction_(shouldRemoveUpdateAction)
    , Logger(logger)
{ }

template <class TKey, class TUpdateParameters>
void TUpdateExecutor<TKey, TUpdateParameters>::StartPeriodicUpdates(const IInvokerPtr& invoker, TDuration updatePeriod)
{
    UpdateExecutor_ = New<NConcurrency::TPeriodicExecutor>(
        invoker,
        BIND(&TUpdateExecutor<TKey, TUpdateParameters>::ExecuteUpdates, MakeWeak(this), invoker),
        updatePeriod,
        NConcurrency::EPeriodicExecutorMode::Automatic);
    UpdateExecutor_->Start();
}

template <class TKey, class TUpdateParameters>
void TUpdateExecutor<TKey, TUpdateParameters>::StopPeriodicUpdates()
{
    UpdateExecutor_->Stop();
    UpdateExecutor_.Reset();
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
void TUpdateExecutor<TKey, TUpdateParameters>::ExecuteUpdates(IInvokerPtr invoker)
{
    VERIFY_THREAD_AFFINITY(UpdateThread);

    LOG_INFO("Updating items (Count: %v)", Updates_.size());

    std::vector<TKey> updatesToRemove;
    std::vector<TFuture<void>> asyncResults;
    for (auto& pair : Updates_) {
        const auto& key = pair.first;
        auto& updateRecord = pair.second;
        if (ShouldRemoveUpdateAction_(&updateRecord.UpdateParameters)) {
            updatesToRemove.push_back(key);
        } else {
            LOG_DEBUG("Updating item (Key: %v)", key);
            asyncResults.push_back(
                DoExecuteUpdate(&updateRecord).Apply(
                    BIND(&TUpdateExecutor<TKey, TUpdateParameters>::OnUpdateExecuted, MakeStrong(this), key)
                        .AsyncVia(invoker)));
        }
    }

    // Cleanup.
    for (auto key : updatesToRemove) {
        RemoveUpdate(key);
    }

    auto result = NConcurrency::WaitFor(Combine(asyncResults));
    if (!result.IsOK()) {
        LOG_ERROR(result, "Update failed");
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
TFuture<void> TUpdateExecutor<TKey, TUpdateParameters>::DoExecuteUpdate(TUpdateRecord* updateRecord)
{
    VERIFY_THREAD_AFFINITY(UpdateThread);

    auto lastUpdateFuture = updateRecord->LastUpdateFuture.Apply(
        CreateUpdateAction_(updateRecord->Key, &updateRecord->UpdateParameters));
    updateRecord->LastUpdateFuture = std::move(lastUpdateFuture);
    return updateRecord->LastUpdateFuture;
}

template <class TKey, class TUpdateParameters>
void TUpdateExecutor<TKey, TUpdateParameters>::OnUpdateExecuted(const TKey& key)
{
    VERIFY_THREAD_AFFINITY(UpdateThread);

    LOG_DEBUG("Item updated (Key: %v)", key);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
