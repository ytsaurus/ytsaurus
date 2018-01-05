#pragma once

#include "public.h"

#include <yt/core/concurrency/periodic_executor.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TUpdateParameters>
class TUpdateExecutor
    : public TRefCounted
{
public:
    TUpdateExecutor(
        IInvokerPtr invoker,
        TCallback<TCallback<TFuture<void>()>(const TKey&, TUpdateParameters*)> createUpdateAction,
        TCallback<bool(const TUpdateParameters*)> shouldRemoveUpdateAction,
        TCallback<void(const TError&)> onUpdateFailed,
        TDuration period,
        NLogging::TLogger logger);

    void Start();
    void Stop();
    void SetPeriod(TDuration period);

    TUpdateParameters* AddUpdate(const TKey& key, const TUpdateParameters& parameters);
    void RemoveUpdate(const TKey& key);

    TUpdateParameters* GetUpdate(const TKey& key);
    TUpdateParameters* FindUpdate(const TKey& key);

    void Clear();

    TFuture<void> ExecuteUpdate(const TKey& key);

private:
    const TCallback<TCallback<TFuture<void>()>(const TKey&, TUpdateParameters*)> CreateUpdateAction_;
    const TCallback<bool(const TUpdateParameters*)> ShouldRemoveUpdateAction_;
    const TCallback<void(const TError&)> OnUpdateFailed_;
    const NLogging::TLogger Logger;

    const NConcurrency::TPeriodicExecutorPtr UpdateExecutor_;

    struct TUpdateRecord
    {
        TUpdateRecord(const TKey& key, const TUpdateParameters& parameters)
            : Key(key)
            , UpdateParameters(parameters)
        { }

        TKey Key;
        TUpdateParameters UpdateParameters;
        TFuture<void> LastUpdateFuture = VoidFuture;
    };

    yhash<TKey, TUpdateRecord> Updates_;

    DECLARE_THREAD_AFFINITY_SLOT(UpdateThread);

    TUpdateRecord* FindUpdateRecord(const TKey& key);
    TFuture<void> DoExecuteUpdate(TUpdateRecord* updateRecord);
    void ExecuteUpdates();
    void OnUpdateExecuted(const TKey& key);
    TCallback<TFuture<void>()> CreateUpdateAction(const TKey& key, TUpdateParameters* updateParameters);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define UPDATE_EXECUTOR_INL_H_
#include "update_executor-inl.h"
#undef UPDATE_EXECUTOR_INL_H_
