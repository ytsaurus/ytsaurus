#include "archive_reporter.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/nonblocking_batcher.h>

#include <yt/yt/core/utilex/random.h>

namespace NYT::NServer {

using namespace NApi;
using namespace NConcurrency;
using namespace NProfiling;
using namespace NTableClient;
using namespace NLogging;
using namespace NTransactionClient;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class TLimiter
{
public:
    explicit TLimiter(i64 maxValue)
        : MaxValue_(maxValue)
    { }

    bool TryIncrease(i64 delta)
    {
        if (Value_.fetch_add(delta, std::memory_order::relaxed) + delta <= GetMaxValue()) {
            return true;
        }
        Decrease(delta);
        return false;
    }

    void Decrease(i64 delta)
    {
        // TODO(dakovalkov): Is it possible?
        if (Value_.fetch_sub(delta, std::memory_order::relaxed) < delta) {
            // rollback operation on negative result
            // negative result can be in case of batcher data dropping and on-the-fly transaction
            Value_.fetch_add(delta, std::memory_order::relaxed);
        }
    }

    void Reset()
    {
        Value_.store(0, std::memory_order::relaxed);
    }

    i64 GetValue() const
    {
        return Value_.load(std::memory_order::relaxed);
    }

    i64 GetMaxValue() const
    {
        return MaxValue_.load(std::memory_order::relaxed);
    }

    void UpdateMaxValue(i64 newMax) noexcept
    {
        MaxValue_.store(newMax, std::memory_order::relaxed);
    }

private:
    std::atomic<i64> MaxValue_;
    std::atomic<i64> Value_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

static YT_DEFINE_GLOBAL(const NLogging::TLogger, ArchiveReporterLogger, "ArchiveReporter");
static constexpr int QueueIsTooLargeMultiplier = 2;

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

int TArchiveVersionHolder::Get() const
{
    return Version_.load(std::memory_order::relaxed);
}

void TArchiveVersionHolder::Set(int version)
{
    Version_.store(version, std::memory_order::relaxed);
}

////////////////////////////////////////////////////////////////////////////////

class TArchiveReporter
    : public IArchiveReporter
{
public:
    using TBatch = std::vector<std::unique_ptr<IArchiveRowlet>>;

    TArchiveReporter(
        TArchiveVersionHolderPtr version,
        TArchiveReporterConfigPtr reporterConfig,
        TArchiveHandlerConfigPtr handlerConfig,
        TNameTablePtr nameTable,
        TString reporterName,
        NNative::IClientPtr client,
        IInvokerPtr invoker,
        const TProfiler& profiler)
        : Logger(NServer::NDetail::ArchiveReporterLogger().WithTag("ReporterName: %v", std::move(reporterName)))
        , ReporterConfig_(std::move(reporterConfig))
        , HandlerConfig_(std::move(handlerConfig))
        , NameTable_(std::move(nameTable))
        , Version_(std::move(version))
        , Client_(std::move(client))
        , Limiter_(HandlerConfig_.Acquire()->MaxInProgressDataSize)
        , Batcher_(New<TNonblockingBatcher<std::unique_ptr<IArchiveRowlet>>>(TBatchSizeLimiter(ReporterConfig_.Acquire()->MaxItemsInBatch), ReporterConfig_.Acquire()->ReportingPeriod))
        , EnqueuedCounter_(profiler.Counter("/enqueued"))
        , DequeuedCounter_(profiler.Counter("/dequeued"))
        , DroppedCounter_(profiler.Counter("/dropped"))
        , WeightViolatedDroppedCounter_(profiler.Counter("/weight_violated_dropped"))
        , WriteFailuresCounter_(profiler.Counter("/write_failures"))
        , PendingCounter_(profiler.Gauge("/pending"))
        , QueueIsTooLargeCounter_(profiler.Gauge("/queue_is_too_large"))
        , CommittedCounter_(profiler.Counter("/committed"))
        , CommittedDataWeightCounter_(profiler.Counter("/committed_data_weight"))
    {
        BIND(&TArchiveReporter::Loop, MakeWeak(this))
            .Via(invoker)
            .Run();
        EnableSemaphore_->Acquire();
        SetEnabled(ReporterConfig_.Acquire()->Enabled);
    }

    void OnConfigChanged(
        const TArchiveReporterConfigPtr& newReporterConfig,
        const TArchiveHandlerConfigPtr& newHandlerConfig) override
    {
        Limiter_.UpdateMaxValue(newHandlerConfig->MaxInProgressDataSize);
        Batcher_->UpdateSettings(
            newReporterConfig->ReportingPeriod,
            TBatchSizeLimiter(newReporterConfig->MaxItemsInBatch),
            /*allowEmptyBatches*/ false);
        SetEnabled(newReporterConfig->Enabled);

        ReporterConfig_.Store(newReporterConfig);
        HandlerConfig_.Store(newHandlerConfig);
    }

    void Enqueue(std::unique_ptr<IArchiveRowlet> rowlet) override
    {
        if (!IsEnabled()) {
            return;
        }
        if (Limiter_.TryIncrease(rowlet->EstimateSize())) {
            Batcher_->Enqueue(std::move(rowlet));
            PendingCounter_.Update(PendingCount_++);
            EnqueuedCounter_.Increment();
            UpdateQueueIsTooLargeCounter();
        } else {
            DroppedCount_.fetch_add(1, std::memory_order::relaxed);
            DroppedCounter_.Increment();
        }
    }

    void SetEnabled(bool enable) override
    {
        bool oldEnable = Enabled_.exchange(enable);
        if (oldEnable != enable) {
            enable ? DoEnable() : DoDisable();
        }
    }

    int ExtractWriteFailuresCount() override
    {
        return WriteFailuresCount_.exchange(0);
    }

    bool IsQueueTooLarge() const override
    {
        return NServer::NDetail::QueueIsTooLargeMultiplier * Limiter_.GetValue() > Limiter_.GetMaxValue();
    }

private:
    const TLogger Logger;
    TAtomicIntrusivePtr<TArchiveReporterConfig> ReporterConfig_;
    TAtomicIntrusivePtr<TArchiveHandlerConfig> HandlerConfig_;
    const TNameTablePtr NameTable_;
    const TArchiveVersionHolderPtr Version_;
    const NNative::IClientPtr Client_;

    NServer::NDetail::TLimiter Limiter_;
    TNonblockingBatcherPtr<std::unique_ptr<IArchiveRowlet>> Batcher_;

    TCounter EnqueuedCounter_;
    TCounter DequeuedCounter_;
    TCounter DroppedCounter_;
    TCounter WeightViolatedDroppedCounter_;
    TCounter WriteFailuresCounter_;
    TGauge PendingCounter_;
    TGauge QueueIsTooLargeCounter_;
    TCounter CommittedCounter_;
    TCounter CommittedDataWeightCounter_;

    const TAsyncSemaphorePtr EnableSemaphore_ = New<TAsyncSemaphore>(1);

    std::atomic<bool> Enabled_ = false;
    std::atomic<int> PendingCount_ = 0;
    std::atomic<int> DroppedCount_ = 0;
    std::atomic<int> WriteFailuresCount_ = 0;

    void Loop()
    {
        while (true) {
            WaitForEnabled();
            auto asyncBatch = Batcher_->DequeueBatch();
            auto batch = WaitForUnique(asyncBatch)
                .ValueOrThrow();

            if (batch.empty()) {
                continue;
            }

            PendingCounter_.Update(PendingCount_ -= batch.size());
            DequeuedCounter_.Increment(batch.size());
            WriteBatchWithExpBackoff(batch);
        }
    }

    void WriteBatchWithExpBackoff(const TBatch& batch)
    {
        TTraceContextGuard traceContextGuard(TTraceContext::NewRoot("ReportRowsToArchive"));

        const auto& reporterConfig = ReporterConfig_.Acquire();
        auto delay = reporterConfig->MinRepeatDelay;
        auto maxRepeatDelay = reporterConfig->MaxRepeatDelay;
        while (IsEnabled()) {
            auto dropped = DroppedCount_.exchange(0);
            if (dropped > 0) {
                YT_LOG_WARNING("Maximum items reached, dropping archived rows (DroppedItems: %v)", dropped);
            }
            try {
                TryHandleBatch(batch);
                i64 dataSize = 0;
                for (const auto& rowlet : batch) {
                    dataSize += rowlet->EstimateSize();
                }
                Limiter_.Decrease(dataSize);
                UpdateQueueIsTooLargeCounter();
                return;
            } catch (const std::exception& ex) {
                WriteFailuresCount_.fetch_add(1, std::memory_order::relaxed);
                WriteFailuresCounter_.Increment();
                YT_LOG_WARNING(ex, "Failed to upload archived rows (RetryDelay: %v, PendingItems: %v)",
                    delay.Seconds(),
                    GetPendingCount());
            }
            TDelayedExecutor::WaitForDuration(RandomDuration(delay));
            delay = std::min(maxRepeatDelay, delay * 2);
        }
    }

    void TryHandleBatch(const TBatch& batch)
    {
        YT_LOG_DEBUG("Archive table transaction starting (Items: %v, PendingItems: %v, ArchiveVersion: %v)",
            batch.size(),
            GetPendingCount(),
            Version_->Get());

        TTransactionStartOptions transactionOptions;
        transactionOptions.Atomicity = EAtomicity::None;
        auto asyncTransaction = Client_->StartTransaction(ETransactionType::Tablet, transactionOptions);
        auto transactionOrError = WaitFor(asyncTransaction);
        auto transaction = transactionOrError.ValueOrThrow();

        YT_LOG_DEBUG("Archive table transaction started (TransactionId: %v, Items: %v)",
            transaction->GetId(),
            batch.size());

        size_t dataWeight = HandleBatchTransaction(*transaction, batch);

        WaitFor(transaction->Commit())
            .ThrowOnError();

        CommittedCounter_.Increment();
        CommittedDataWeightCounter_.Increment(dataWeight);

        YT_LOG_DEBUG("Archive table transaction committed (TransactionId: %v, "
            "CommittedItems: %v, CommittedDataWeight: %v)",
            transaction->GetId(),
            batch.size(),
            dataWeight);
    }

    //! Returns data weight of written batch inside transaction.
    i64 HandleBatchTransaction(ITransaction& transaction, const TBatch& batch)
    {
        int archiveVersion = Version_->Get();
        std::vector<TUnversionedRow> rows;
        std::vector<TUnversionedOwningRow> owningRows;

        i64 dataWeight = 0;
        for (const auto& rowlet : batch) {
            auto row = rowlet->ToRow(archiveVersion);
            bool isValueWeightViolated = IsValueWeightViolated(row);
            if (row && !isValueWeightViolated) {
                dataWeight += GetDataWeight(row.Get());
                rows.push_back(row.Get());
                owningRows.push_back(std::move(row));
            }
            if (isValueWeightViolated) {
                WeightViolatedDroppedCounter_.Increment();
            }
        }

        transaction.WriteRows(
            HandlerConfig_.Acquire()->Path,
            NameTable_,
            MakeSharedRange(std::move(rows), std::move(owningRows)));

        return dataWeight;
    }

    int GetPendingCount() const
    {
        return PendingCount_.load();
    }

    void DoEnable()
    {
        EnableSemaphore_->Release();
        YT_LOG_INFO("Archive reporter enabled");
    }

    void DoDisable()
    {
        EnableSemaphore_->Acquire();
        Batcher_->Drop();
        Limiter_.Reset();
        DroppedCount_.store(0, std::memory_order::relaxed);
        PendingCounter_.Update(PendingCount_ = 0);
        QueueIsTooLargeCounter_.Update(0);
        YT_LOG_INFO("Archive reporter disabled");
    }

    bool IsEnabled() const
    {
        return EnableSemaphore_->IsReady();
    }

    void WaitForEnabled() const
    {
        if (IsEnabled()) {
            return;
        }

        YT_LOG_INFO("Waiting for archive reporter to become enabled");
        WaitFor(EnableSemaphore_->GetReadyEvent())
            .ThrowOnError();
        YT_LOG_INFO("Archive reporter became enabled, resuming archive uploading");
    }

    bool IsValueWeightViolated(TUnversionedRow row) const
    {
        for (auto value : row) {
            auto valueWeight = GetDataWeight(value);
            if (valueWeight > MaxStringValueLength) {
                YT_LOG_WARNING(
                    "Archive table row violates value data weight, archivation skipped "
                    "(Key: %v, Weight: %v, WeightLimit: %v)",
                    NameTable_->GetNameOrThrow(value.Id),
                    valueWeight,
                    MaxStringValueLength);
                return true;
            }
        }
        return false;
    }

    void UpdateQueueIsTooLargeCounter() const
    {
        QueueIsTooLargeCounter_.Update(IsQueueTooLarge() ? 1 : 0);
    }
};

////////////////////////////////////////////////////////////////////////////////

IArchiveReporterPtr CreateArchiveReporter(
    TArchiveVersionHolderPtr version,
    TArchiveReporterConfigPtr reporterConfig,
    TArchiveHandlerConfigPtr handlerConfig,
    TNameTablePtr nameTable,
    TString reporterName,
    NNative::IClientPtr client,
    IInvokerPtr invoker,
    TProfiler profiler)
{
    return New<TArchiveReporter>(
        std::move(version),
        std::move(reporterConfig),
        std::move(handlerConfig),
        std::move(nameTable),
        std::move(reporterName),
        std::move(client),
        std::move(invoker),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServer
