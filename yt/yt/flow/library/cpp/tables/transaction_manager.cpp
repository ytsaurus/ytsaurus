#include "transaction_manager.h"

#include <yt/yt/flow/library/cpp/common/computation.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/misc/retryable_client.h>
#include <yt/yt/flow/library/cpp/misc/retryable_transaction.h>

#include <yt/yt/flow/library/cpp/native_client/public.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/misc/backoff_strategy.h>
#include <yt/yt/core/misc/jitter.h>

#include <yt/yt/core/ypath/helpers.h>

#include <util/random/random.h>

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;
using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TTransactionManager(
    TTransactionManagerContextPtr context,
    TDynamicRetryableRequestSpecPtr spec)
    : Context_(std::move(context))
    , Spec_(std::move(spec))
    , PartitionTransactionsPath_(NYPath::YPathJoin(Context_->PipelinePath.GetPath(), PartitionTransactionsTableName))
    , Logger(Context_->Logger.WithTag("TransactionManager"))
    , Profiler_(Context_->Profiler.WithPrefix("/transaction_manager"))
    , SkipEmptyTransactionsDeadline_(TInstant::Zero())
    , InitialEmptyTransactionCompleted_(false)
    , CommitTransactionErrorState_(Context_->StatusProfiler->ErrorState("/commit_transaction"))
    , CommitTimer_(Profiler_.Timer("/commit_time"))
    , CommitTotalCounter_(Profiler_.Counter("/commit_total"))
    , CommitFailedCounter_(Profiler_.Counter("/commit_failed"))
    , CommitSkippedEmptyCounter_(Profiler_.Counter("/commit_skipped_empty"))
{ }

void TTransactionManager::Reconfigure(TDynamicRetryableRequestSpecPtr spec)
{
    Spec_.Store(std::move(spec));
}

IRetryableTransactionPtr TTransactionManager::CreateTransaction()
{
    return CreateRetryableTransaction();
}

TInstant TTransactionManager::ComputeDeadlineWithJitter(TDuration duration)
{
    auto randomGenerator = [] {
        return RandomNumber<double>() * 2 - 1.0;
    };
    return TInstant::Now() + ApplyJitter(duration, 0.2, randomGenerator);
}

ITransactionPtr TTransactionManager::CreateOrdinaryTransactionSync(TDuration timeout)
{
    TTransactionStartOptions startOptions;
    auto attributes = NYTree::CreateEphemeralAttributes();
    attributes->Set("title", Format("Flow: transaction manager transaction of partition %v", Context_->PartitionId));
    startOptions.Attributes = std::move(attributes);
    startOptions.Timeout = timeout;
    auto ordinaryTransaction = WaitFor(Context_->Client->StartTransaction(
        NTransactionClient::ETransactionType::Tablet,
        startOptions))
        .ValueOrThrow();
    return ordinaryTransaction;
}

TTransactionCommitResult TTransactionManager::CommitOrdinaryTransactionSync(ITransactionPtr ordinaryTransaction, TDuration timeout)
{
    TTransactionCommitOptions commitOptions;
    commitOptions.PrerequisiteTransactionIds = {Context_->LeaseId};
    auto timeoutOptions = TFutureTimeoutOptions{.Error = TError(NYT::EErrorCode::Timeout, "Timeout (inner timeout of retryable client)")};
    return WaitFor(ordinaryTransaction->Commit(commitOptions).WithTimeout(timeout, timeoutOptions))
        .ValueOrThrow();
}

// If committer throws, consider retry.
// If committer returns error, it is final error.
TError TTransactionManager::DoCommitWithRetriesSync(TDynamicRetryableRequestSpecPtr spec, std::function<TError(TDuration)> committer)
{
    YT_VERIFY(GetCurrentInvoker());

    auto startTime = TInstant::Now();

    TError lastError;
    auto backoffStrategy = TBackoffStrategy(spec->Backoff);
    auto innerTimeout = spec->MinInnerTimeout;

    auto enrichError = [&] (TError error) -> TError {
        return error
            << TErrorAttribute("total_time", TInstant::Now() - startTime)
            << TErrorAttribute("total_time_limit", spec->Timeout)
            << TErrorAttribute("failed_attempts", backoffStrategy.GetInvocationIndex())
            << TErrorAttribute("attempts_count_limit", backoffStrategy.GetInvocationCount());
    };

    auto makeNoMoreRetriesError = [&] () -> TError {
        auto resultError = enrichError(TError("Common attempts timeout exceeded or attempts count limit exceeded"));
        if (!lastError.IsOK()) {
            resultError <<= lastError;
        }
        return resultError;
    };

    try {
        while (true) {
            i64 attempt = backoffStrategy.GetInvocationIndex();
            CommitTotalCounter_.Increment();
            try {
                auto error = committer(innerTimeout);
                if (!error.IsOK()) {
                    return enrichError(error) << lastError;
                }
                CommitTimer_.Record(TInstant::Now() - startTime);
                CommitTransactionErrorState_->ClearError();
                return TError();
            } catch (const TErrorException& exception) {
                CommitFailedCounter_.Increment();
                if (exception.Error().GetCode() == NYT::EErrorCode::Canceled) {
                    return makeNoMoreRetriesError();
                }

                lastError = exception.Error();

                if (!IsFlowRetriableError(lastError)) {
                    return enrichError(TError("Commit attempt failed, error is not retryable")) << lastError;
                }

                if (!backoffStrategy.Next()) {
                    return makeNoMoreRetriesError();
                }

                CommitTransactionErrorState_->SetError(exception.Error());

                auto backoff = backoffStrategy.GetBackoff();
                innerTimeout = std::max(spec->MinInnerTimeout, backoff);
                YT_TLOG_WARNING("Commit attempt failed with retryable error")
                    .With("Attempt", attempt)
                    .With("SleepDuration", backoff)
                    .With("NextInnerTimeout", innerTimeout)
                    .With(lastError);
                TDelayedExecutor::WaitForDuration(backoff);
            }
        }
    } catch (const TFiberCanceledException&) {
        CommitFailedCounter_.Increment();
        return makeNoMoreRetriesError();
    }
    Y_UNREACHABLE();
}

TFuture<void> TTransactionManager::CommitTransactionImpl(IRetryableTransactionPtr retryableTransaction, TDynamicRetryableRequestSpecPtr spec)
{
    YT_VERIFY(GetCurrentInvoker());

    bool emptyTransaction = retryableTransaction->IsEmpty();

    // Sometimes empty transaction is performed to check job lease.
    if (emptyTransaction && TInstant::Now() < SkipEmptyTransactionsDeadline_.load()) {
        CommitTotalCounter_.Increment();
        CommitSkippedEmptyCounter_.Increment();
        YT_TLOG_INFO("Skip committing because transaction is empty");
        return OKFuture;
    }

    if (!InitialEmptyTransactionCompleted_.load()) {
        auto committer = [&] (TDuration innerTimeout) {
            auto ordinaryTransaction = CreateOrdinaryTransactionSync(innerTimeout);
            // Main action. Write initial "empty" transaction with transaction start timestamp.
            PersistTransactionStartTimestamp(ordinaryTransaction);
            YT_TLOG_INFO("Committing initial empty transaction")
                .With("TransactionStartTimestamp", ordinaryTransaction->GetStartTimestamp());
            CommitOrdinaryTransactionSync(ordinaryTransaction, innerTimeout);
            YT_TLOG_INFO("Committed empty initial transaction")
                .With("TransactionStartTimestamp", ordinaryTransaction->GetStartTimestamp());
            return TError();
        };
        if (auto error = DoCommitWithRetriesSync(spec, committer); !error.IsOK()) {
            return MakeFuture<void>(std::move(error));
        }
        InitialEmptyTransactionCompleted_.store(true);
        SkipEmptyTransactionsDeadline_.store(ComputeDeadlineWithJitter(spec->LeaseCheckPeriod));
    }

    {
        THashSet<NTransactionClient::TTimestamp> previousTransactionStartTimestamps;
        auto committer = [&] (TDuration innerTimeout) {
            auto ordinaryTransaction = CreateOrdinaryTransactionSync(innerTimeout);

            // Prevent double committing.
            // Transaction StartTimestamp is unique id of transaction.
            if (!previousTransactionStartTimestamps.empty() && !emptyTransaction) {
                auto lastSuccessTransactionStartTimestamp = WaitFor(LoadTransactionStartTimestamp(innerTimeout)).ValueOrThrow();
                if (!lastSuccessTransactionStartTimestamp.has_value()) {
                    // It is a rare case because of commiting empty initial transaction.
                    return TError("Commit attempt failed, it impossible to verify if it really failed, so error is not retryable");
                } else if (previousTransactionStartTimestamps.contains(*lastSuccessTransactionStartTimestamp)) {
                    YT_TLOG_INFO("Commit returned error, but transaction was committed")
                        .With("TransactionStartTimestamp", *lastSuccessTransactionStartTimestamp);
                    return TError();
                }
            }

            retryableTransaction->DoAttempt(ordinaryTransaction);
            PersistTransactionStartTimestamp(ordinaryTransaction);
            previousTransactionStartTimestamps.insert(ordinaryTransaction->GetStartTimestamp());

            YT_TLOG_INFO("Committing transaction")
                .With("TransactionStartTimestamp", ordinaryTransaction->GetStartTimestamp())
                .With("PreviousTransactionStartTimestamps", previousTransactionStartTimestamps);
            try {
                auto commitResult = CommitOrdinaryTransactionSync(ordinaryTransaction, innerTimeout);
                retryableTransaction->OnAttemptResult(TCommitAttemptResult{
                    .Transaction = ordinaryTransaction,
                    .CommitResult = commitResult,
                });
                YT_TLOG_INFO("Committed transaction")
                    .With("TransactionStartTimestamp", ordinaryTransaction->GetStartTimestamp());
                return TError();
            } catch (const TErrorException& exception) {
                retryableTransaction->OnAttemptResult(TCommitAttemptResult{
                    .Transaction = ordinaryTransaction,
                    .CommitResult = exception.Error(),
                });
                throw;
            }
        };
        if (auto error = DoCommitWithRetriesSync(spec, committer); !error.IsOK()) {
            return MakeFuture<void>(std::move(error));
        }
        SkipEmptyTransactionsDeadline_.store(ComputeDeadlineWithJitter(spec->LeaseCheckPeriod));
    }
    return OKFuture;
}

TFuture<void> TTransactionManager::CommitTransaction(IRetryableTransactionPtr transaction)
{
    auto spec = Spec_.Acquire();
    return BIND(&TTransactionManager::CommitTransactionImpl, MakeStrong(this), transaction, spec)
        .AsyncVia(GetCurrentInvoker())
        .Run()
        .WithTimeout(
            spec->Timeout,
            TFutureTimeoutOptions{
                .Error = TError(NYT::EErrorCode::Canceled, "Canceled"),
            });
}

TFuture<std::optional<NTransactionClient::TTimestamp>> TTransactionManager::LoadTransactionStartTimestamp(TDuration timeout) const
{
    YT_VERIFY(GetCurrentInvoker());

    auto nameTable = New<TNameTable>();
    i32 partitionIdField = nameTable->GetIdOrRegisterName("partition_id");

    auto rowBuffer = New<TRowBuffer>();

    std::vector<TLegacyKey> keysToLookup;
    auto keyToLookup = rowBuffer->AllocateUnversioned(1);
    keyToLookup[0] = rowBuffer->CaptureValue(MakeUnversionedStringValue(ToString(Context_->PartitionId), partitionIdField));
    keysToLookup.push_back(keyToLookup);

    TLookupRowsOptions lookupRowsOptions;
    lookupRowsOptions.KeepMissingRows = true;
    lookupRowsOptions.Timeout = timeout;
    lookupRowsOptions.Timestamp = NTransactionClient::SyncLastCommittedTimestamp;

    auto range = MakeSharedRange(std::move(keysToLookup), std::move(rowBuffer));

    auto extractTimestamp = [] (TUnversionedLookupRowsResult&& result) -> std::optional<NTransactionClient::TTimestamp> {
        auto rowset = result.Rowset;
        auto rows = rowset->GetRows();
        YT_VERIFY(rows.size() == 1);
        auto row = rows[0];
        auto schema = rowset->GetSchema();
        i32 timestampField = schema->GetColumnIndexOrThrow("last_transaction_start_timestamp");
        if (row) {
            return FromUnversionedValue<std::optional<NTransactionClient::TTimestamp>>(row[timestampField]);
        }
        return std::nullopt;
    };

    auto enrichError = [] (const TErrorOr<std::optional<NTransactionClient::TTimestamp>>& error) {
        if (error.IsOK()) {
            return MakeFuture(error.Value());
        }
        return MakeFuture<std::optional<NTransactionClient::TTimestamp>>(
            TError(error.GetCode(), "Failed to load last success transaction start timestamp")
            << error);
    };

    return Context_->Client->LookupRows(PartitionTransactionsPath_.GetPath(), nameTable, range, lookupRowsOptions)
        .AsUnique()
        .Apply(BIND(extractTimestamp).AsyncVia(GetCurrentInvoker()))
        .Apply(BIND(enrichError));
}

void TTransactionManager::PersistTransactionStartTimestamp(ITransactionPtr transaction) const
{
    YT_VERIFY(GetCurrentInvoker());

    auto nameTable = New<TNameTable>();
    i32 partitionIdField = nameTable->GetIdOrRegisterName("partition_id");
    i32 timestampField = nameTable->GetIdOrRegisterName("last_transaction_start_timestamp");

    auto rowBuffer = New<TRowBuffer>();
    std::vector<TRowModification> rows;
    {
        auto row = rowBuffer->AllocateUnversioned(2);
        row[0] = rowBuffer->CaptureValue(MakeUnversionedStringValue(ToString(Context_->PartitionId), partitionIdField));
        row[1] = rowBuffer->CaptureValue(MakeUnversionedUint64Value(transaction->GetStartTimestamp(), timestampField));
        rows.push_back(NRowModifications::TWriteRow(row));
    }
    transaction->ModifyRows(PartitionTransactionsPath_.GetPath(), nameTable, MakeSharedRange(std::move(rows), std::move(rowBuffer)));
}

void TTransactionManager::ClearTransactionStartTimestamp(IDynamicTableTransactionPtr transaction) const
{
    YT_VERIFY(GetCurrentInvoker());

    auto nameTable = New<TNameTable>();
    i32 partitionIdField = nameTable->GetIdOrRegisterName("partition_id");

    auto rowBuffer = New<TRowBuffer>();
    std::vector<TRowModification> rows;
    {
        auto row = rowBuffer->AllocateUnversioned(1);
        row[0] = rowBuffer->CaptureValue(MakeUnversionedStringValue(ToString(Context_->PartitionId), partitionIdField));
        rows.push_back(NRowModifications::TDeleteRow(row));
    }
    transaction->ModifyRows(PartitionTransactionsPath_.GetPath(), nameTable, MakeSharedRange(std::move(rows), std::move(rowBuffer)));
}

TFuture<void> TTransactionManager::Cleanup()
{
    return BIND([this, this_ = MakeStrong(this)] {
        auto committer = [&] (TDuration innerTimeout) {
            auto ordinaryTransaction = CreateOrdinaryTransactionSync(innerTimeout);
            ClearTransactionStartTimestamp(ordinaryTransaction);
            YT_TLOG_INFO("Committing finalizing clearing transaction")
                .With("TransactionStartTimestamp", ordinaryTransaction->GetStartTimestamp());
            CommitOrdinaryTransactionSync(ordinaryTransaction, innerTimeout);
            YT_TLOG_INFO("Committed finalizing clearing transaction")
                .With("TransactionStartTimestamp", ordinaryTransaction->GetStartTimestamp());
            return TError();
        };
        if (auto error = DoCommitWithRetriesSync(Spec_.Acquire(), committer); !error.IsOK()) {
            return MakeFuture<void>(std::move(error));
        }
        return OKFuture;
    })
        .AsyncVia(GetCurrentInvoker())
        .Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
