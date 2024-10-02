#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/async_semaphore.h>

#include <optional>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <typename TRequestCommonOptions>
std::optional<bool> AllowFullScanFromOptions(const TRequestCommonOptions& options);

////////////////////////////////////////////////////////////////////////////////

class TTransactionWrapper final
{
public:
    TTransactionWrapper(
        const TTransactionManagerPtr& transactionManager,
        TTransactionId id,
        std::optional<TDuration> requestTimeout,
        std::optional<bool> allowFullScan,
        bool mustOwn,
        bool flushPerformanceStatistics = true);

    ~TTransactionWrapper();

    const TTransactionPtr& Unwrap() const;

    std::optional<TTimestamp> CommitIfOwned() const;

private:
    bool Owned_ = false;
    TTransactionPtr Transaction_;
    NConcurrency::TAsyncSemaphoreGuard LockGuard_;
};

////////////////////////////////////////////////////////////////////////////////

class TReadOnlyTransactionWrapper final
{
public:
    TReadOnlyTransactionWrapper(
        const TTransactionManagerPtr& transactionManager,
        TTimestampOrTransactionId timestampOrTransactionId,
        std::optional<TDuration> requestTimeout,
        std::optional<bool> allowFullScan,
        bool flushPerformanceStatistics = true);

    const TTransactionPtr& Unwrap() const;

private:
    TTransactionPtr Transaction_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define TRANSACTION_WRAPPER_INL_H_
#include "transaction_wrapper-inl.h"
#undef TRANSACTION_WRAPPER_INL_H_
