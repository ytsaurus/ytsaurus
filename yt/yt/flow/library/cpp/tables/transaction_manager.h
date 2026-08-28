#pragma once

#include "public.h"

#include "context.h"

#include <yt/yt/flow/library/cpp/common/dyntable_lease.h>
#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/misc/public.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/misc/backoff_strategy.h>

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

struct TTransactionManagerContext
    : public TContext
{
    NTransactionClient::TTransactionId LeaseId;
    TPartitionId PartitionId;
    IStatusProfilerPtr StatusProfiler;

    //! When set, commits are fenced by the pipeline's leases dynamic table (validate + touch the
    //! partition lease rows inside every transaction) instead of the LeaseId prerequisite.
    bool DyntableLease = false;
    TJobId JobId;
};

DEFINE_REFCOUNTED_TYPE(TTransactionManagerContext);

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager
    : public TRefCounted
{
public:
    TTransactionManager(
        TTransactionManagerContextPtr context,
        TDynamicRetryableRequestSpecPtr spec);

    void Reconfigure(TDynamicRetryableRequestSpecPtr spec);

    IRetryableTransactionPtr CreateTransaction();
    TFuture<void> CommitTransaction(IRetryableTransactionPtr transaction);

    // Clear temporary state on YT after the job is completed.
    TFuture<void> Cleanup();

private:
    NApi::ITransactionPtr CreateOrdinaryTransactionSync(TDuration timeout);
    NApi::TTransactionCommitResult CommitOrdinaryTransactionSync(NApi::ITransactionPtr ordinaryTransaction, TDuration timeout);

    TError DoCommitWithRetriesSync(TDynamicRetryableRequestSpecPtr spec, std::function<TError(TDuration)> committer);

    TFuture<void> CommitTransactionImpl(IRetryableTransactionPtr retryableTransaction, TDynamicRetryableRequestSpecPtr spec);

    TFuture<std::optional<NTransactionClient::TTimestamp>> LoadTransactionStartTimestamp(TDuration timeout) const;
    void PersistTransactionStartTimestamp(NApi::ITransactionPtr transaction) const;
    void ClearTransactionStartTimestamp(NApi::IDynamicTableTransactionPtr transaction) const;

    static TInstant ComputeDeadlineWithJitter(TDuration duration);

private:
    const TTransactionManagerContextPtr Context_;
    TAtomicIntrusivePtr<TDynamicRetryableRequestSpec> Spec_;
    const NYPath::TRichYPath PartitionTransactionsPath_;
    const TDyntableLeases DyntableLeases_;
    const NLogging::TLogger Logger;
    const NProfiling::TProfiler Profiler_;

    std::atomic<TInstant> SkipEmptyTransactionsDeadline_;
    std::atomic<bool> InitialEmptyTransactionCompleted_;

    //! The pipeline-wide lease deadline this job last read, kept so that the commits in between
    //! do not read the row it lives in (see TDyntableLeases::ValidateAndTouchPartitionLease).
    std::atomic<TInstant> DyntableLeaseDeadline_ = TInstant::Zero();

    IStatusErrorStatePtr CommitTransactionErrorState_;

    NProfiling::TEventTimer CommitTimer_;
    NProfiling::TCounter CommitTotalCounter_;
    NProfiling::TCounter CommitFailedCounter_;
    NProfiling::TCounter CommitSkippedEmptyCounter_;
};

DEFINE_REFCOUNTED_TYPE(TTransactionManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
