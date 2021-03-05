#include "archive_reporter.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/nonblocking_batch.h>
#include <yt/yt/core/utilex/random.h>

namespace NYT {

using namespace NApi;
using namespace NConcurrency;
using namespace NProfiling;
using namespace NTableClient;
using namespace NLogging;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class TLimiter
{
public:
    explicit TLimiter(ui64 maxValue)
        : MaxValue_(maxValue)
    { }

    bool TryIncrease(ui64 delta)
    {
        if (Value_.fetch_add(delta, std::memory_order_relaxed) + delta <= MaxValue_) {
            return true;
        }
        Decrease(delta);
        return false;
    }

    void Decrease(ui64 delta)
    {
        // TODO(dakovalkov): Is it possible?
        if (Value_.fetch_sub(delta, std::memory_order_relaxed) < delta) {
            // rollback operation on negative result
            // negative result can be in case of batcher data dropping and on-the-fly transaction
            Value_.fetch_add(delta, std::memory_order_relaxed);
        }
    }

    void Reset()
    {
        Value_.store(0, std::memory_order_relaxed);
    }

    ui64 GetValue() const
    {
        return Value_.load();
    }

    ui64 GetMaxValue() const
    {
        return MaxValue_;
    }

private:
    const ui64 MaxValue_;
    std::atomic<ui64> Value_ = {0};
};

////////////////////////////////////////////////////////////////////////////////

static const TLogger ArchiveReporterLogger("ArchiveReporter");
static constexpr int QueueIsTooLargeMultiplier = 2;

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

void TArchiveVersionHolder::Set(int version)
{
    Version_.store(version, std::memory_order_relaxed);
}

int TArchiveVersionHolder::Get() const
{
    return Version_.load(std::memory_order_relaxed);
}

////////////////////////////////////////////////////////////////////////////////

class TArchiveReporter::TImpl
    : public TRefCounted
{
public:
    using TBatch = std::vector<std::unique_ptr<IArchiveRowlet>>;

    TImpl(
        TArchiveVersionHolderPtr version,
        TArchiveReporterConfigPtr config,
        TNameTablePtr nameTable,
        TString reporterName,
        NNative::IClientPtr client,
        IInvokerPtr invoker,
        const TRegistry& profiler)
        : Logger(NYT::NDetail::ArchiveReporterLogger.WithTag("ReporterName: %v", std::move(reporterName)))
        , Config_(std::move(config))
        , NameTable_(std::move(nameTable))
        , Version_(std::move(version))
        , Client_(std::move(client))
        , Limiter_(Config_->MaxInProgressDataSize)
        , Batcher_(New<TNonblockingBatch<std::unique_ptr<IArchiveRowlet>>>(Config_->MaxItemsInBatch, Config_->ReportingPeriod))
        , EnqueuedCounter_(profiler.Counter("/enqueued"))
        , DequeuedCounter_(profiler.Counter("/dequeued"))
        , DroppedCounter_(profiler.Counter("/dropped"))
        , WriteFailuresCounter_(profiler.Counter("/write_failures"))
        , PendingCounter_(profiler.Gauge("/pending"))
        , QueueIsTooLargeCounter_(profiler.Gauge("/queue_is_too_large"))
        , CommittedCounter_(profiler.Counter("/committed"))
        , CommittedDataWeightCounter_(profiler.Counter("/committed_data_weight"))
    {
        BIND(&TImpl::Loop, MakeWeak(this))
            .Via(invoker)
            .Run();
        EnableSemaphore_->Acquire();
    }

    void Enqueue(std::unique_ptr<IArchiveRowlet> rowStub)
    {
        if (!IsEnabled()) {
            return;
        }
        if (Limiter_.TryIncrease(rowStub->EstimateSize())) {
            Batcher_->Enqueue(std::move(rowStub));
            PendingCounter_.Update(PendingCount_++);
            EnqueuedCounter_.Increment();
            QueueIsTooLargeCounter_.Update(QueueIsTooLarge() ? 1 : 0);
        } else {
            DroppedCount_.fetch_add(1, std::memory_order_relaxed);
            DroppedCounter_.Increment();
        }
    }

    void SetEnabled(bool enable)
    {
        bool oldEnable = Enabled_.exchange(enable);
        if (oldEnable != enable) {
            enable ? DoEnable() : DoDisable();
        }
    }

    int ExtractWriteFailuresCount()
    {
        return WriteFailuresCount_.exchange(0);
    }

    bool QueueIsTooLarge() const
    {
        return NYT::NDetail::QueueIsTooLargeMultiplier * Limiter_.GetValue() > Limiter_.GetMaxValue();
    }

private:
    TLogger Logger;
    const TArchiveReporterConfigPtr Config_;
    TNameTablePtr NameTable_;
    const TArchiveVersionHolderPtr Version_;
    const NNative::IClientPtr Client_;
    NYT::NDetail::TLimiter Limiter_;
    TNonblockingBatchPtr<std::unique_ptr<IArchiveRowlet>> Batcher_;

    TCounter EnqueuedCounter_;
    TCounter DequeuedCounter_;
    TCounter DroppedCounter_;
    TCounter WriteFailuresCounter_;
    TGauge PendingCounter_;
    TGauge QueueIsTooLargeCounter_;
    TCounter CommittedCounter_;
    TCounter CommittedDataWeightCounter_;

    TAsyncSemaphorePtr EnableSemaphore_ = New<TAsyncSemaphore>(1);
    std::atomic<bool> Enabled_ = {false};
    std::atomic<ui64> PendingCount_ = {0};
    std::atomic<ui64> DroppedCount_ = {0};
    std::atomic<ui64> WriteFailuresCount_ = {0};

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
        auto delay = Config_->MinRepeatDelay;
        while (IsEnabled()) {
            auto dropped = DroppedCount_.exchange(0);
            if (dropped) {
                YT_LOG_WARNING("Maximum items reached, dropping archived rows (DroppedItems: %v)", dropped);
            }
            try {
                TryHandleBatch(batch);
                ui64 dataSize = 0;
                for (auto& rowStub : batch) {
                    dataSize += rowStub->EstimateSize();
                }
                Limiter_.Decrease(dataSize);
                return;
            } catch (const std::exception& ex) {
                WriteFailuresCount_.fetch_add(1, std::memory_order_relaxed);
                WriteFailuresCounter_.Increment();
                YT_LOG_WARNING(ex, "Failed to uppload archived rows (RetryDelay: %v, PendingItems: %v)",
                    delay.Seconds(),
                    GetPendingCount());
            }
            TDelayedExecutor::WaitForDuration(RandomDuration(delay));
            delay *= 2;
            if (delay > Config_->MaxRepeatDelay) {
                delay = Config_->MaxRepeatDelay;
            }
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

    // Must return dataweight of written batch inside transaction.
    size_t HandleBatchTransaction(ITransaction& transaction, const TBatch& batch)
    {
        int archiveVersion = Version_->Get();
        std::vector<TUnversionedRow> rows;
        std::vector<TUnversionedOwningRow> owningRows;

        size_t dataWeight = 0;
        for (auto&& rowStub : batch) {
            if (auto row = rowStub->ToRow(archiveVersion); row && !IsValueWeightViolated(row)) {
                dataWeight += GetDataWeight(row.Get());
                rows.emplace_back(row.Get());
                owningRows.emplace_back(std::move(row));
            }
        }

        transaction.WriteRows(
            Config_->Path,
            NameTable_,
            MakeSharedRange(std::move(rows), std::move(owningRows)));

        return dataWeight;
    }

    ui64 GetPendingCount() const
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
        DroppedCount_.store(0, std::memory_order_relaxed);
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
        auto event = EnableSemaphore_->GetReadyEvent();
        WaitFor(event).ThrowOnError();
        YT_LOG_INFO("Archive reporter became enabled, resuming archive uploading");
    }

    bool IsValueWeightViolated(TUnversionedRow row) const
    {
        for (auto value : row) {
            auto valueWeight = GetDataWeight(value);
            if (valueWeight > MaxStringValueLength) {
                YT_LOG_WARNING(
                    "Archive table row violates value data weight, archivation skipped"
                    "(Key: %v, Weight: %v, WeightLimit: %v)",
                    NameTable_->GetNameOrThrow(value.Id),
                    valueWeight,
                    MaxStringValueLength);
                return true;
            }
        }
        return false;
    };
};

////////////////////////////////////////////////////////////////////////////////

TArchiveReporter::TArchiveReporter(
    TArchiveVersionHolderPtr version,
    TArchiveReporterConfigPtr config,
    TNameTablePtr nameTable,
    TString reporterName,
    NNative::IClientPtr client,
    IInvokerPtr invoker,
    const TRegistry& profiler)
    : Impl_(New<TImpl>(
        std::move(version),
        std::move(config),
        std::move(nameTable),
        std::move(reporterName),
        std::move(client),
        std::move(invoker),
        profiler))
{ }
 
TArchiveReporter::~TArchiveReporter() = default;

void TArchiveReporter::Enqueue(std::unique_ptr<IArchiveRowlet> rowStub)
{
    Impl_->Enqueue(std::move(rowStub));
}

void TArchiveReporter::SetEnabled(bool enable)
{
    Impl_->SetEnabled(enable);
}

int TArchiveReporter::ExtractWriteFailuresCount()
{
    return Impl_->ExtractWriteFailuresCount();
}

bool TArchiveReporter::QueueIsTooLarge() const
{
    return Impl_->QueueIsTooLarge();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
