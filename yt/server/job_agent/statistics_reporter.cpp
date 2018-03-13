#include "statistics_reporter.h"
#include "private.h"
#include "config.h"

#include <yt/server/data_node/master_connector.h>

#include <yt/server/cell_node/bootstrap.h>

#include <yt/ytlib/api/connection.h>
#include <yt/ytlib/api/native_client.h>
#include <yt/ytlib/api/native_connection.h>
#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/tablet_client/table_mount_cache.h>

#include <yt/ytlib/table_client/row_buffer.h>
#include <yt/ytlib/table_client/name_table.h>

#include <yt/ytlib/scheduler/helpers.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/nonblocking_batch.h>

#include <yt/core/profiling/profiler.h>
#include <yt/core/concurrency/async_semaphore.h>
#include <yt/core/utilex/random.h>

namespace NYT {
namespace NJobAgent {

using namespace NNodeTrackerClient;
using namespace NTransactionClient;
using namespace NYson;
using namespace NYTree;
using namespace NCellNode;
using namespace NConcurrency;
using namespace NApi;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NProfiling;
using namespace NScheduler;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

struct TJobTag
{ };

struct TJobSpecTag
{ };

namespace {

static const TProfiler JobProfiler("/statistics_reporter/jobs");
static const TProfiler JobSpecProfiler("/statistics_reporter/job_specs");
static const TLogger ReporterLogger("JobReporter");

////////////////////////////////////////////////////////////////////////////////

bool IsSpecEntry(const TJobStatistics& stat)
{
    return stat.Spec().HasValue() || stat.Type().HasValue();
}

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

private:
    const ui64 MaxValue_;
    std::atomic<ui64> Value_ = {0};
};

////////////////////////////////////////////////////////////////////////////////

using TBatch = std::vector<TJobStatistics>;

class TSharedData
    : public TRefCounted
{
public:
    void SetOperationArchiveVersion(int version)
    {
        Version_.store(version, std::memory_order_relaxed);
    }

    int GetOperationArchiveVersion() const
    {
        return Version_.load(std::memory_order_relaxed);
    }

private:
    std::atomic<int> Version_ = {-1};
};

DECLARE_REFCOUNTED_TYPE(TSharedData)
DEFINE_REFCOUNTED_TYPE(TSharedData)

////////////////////////////////////////////////////////////////////////////////

class THandlerBase
    : public TRefCounted
{
public:
    THandlerBase(
        TSharedDataPtr data,
        const TStatisticsReporterConfigPtr& config,
        const TString& reporterName,
        INativeClientPtr client,
        IInvokerPtr invoker,
        const TProfiler& profiler,
        ui64 maxInProgressDataSize)
        : Data_(std::move(data))
        , Config_(config)
        , Client_(std::move(client))
        , Profiler_(profiler)
        , Limiter_(maxInProgressDataSize)
        , Batcher_(Config_->MaxItemsInBatch, Config_->ReportingPeriod)
    {
        BIND(&THandlerBase::Loop, MakeWeak(this))
            .Via(invoker)
            .Run();
        EnableSemaphore_.Acquire();
        Logger.AddTag("Reporter: %v", reporterName);
    }

    void Enqueue(TJobStatistics&& statistics)
    {
        if (!IsEnabled()) {
            return;
        }
        if (Limiter_.TryIncrease(statistics.EstimateSize())) {
            Batcher_.Enqueue(std::move(statistics));
            Profiler_.Increment(PendingCounter_, 1);
            Profiler_.Increment(EnqueuedCounter_);
        } else {
            DroppedCount_.fetch_add(1, std::memory_order_relaxed);
            Profiler_.Increment(DroppedCounter_);
        }
    }

    void SetEnabled(bool enable)
    {
        bool oldEnable = Enabled_.exchange(enable);
        if (oldEnable != enable) {
            enable ? DoEnable() : DoDisable();
        }
    }

    const TSharedDataPtr& GetSharedData()
    {
        return Data_;
    }

protected:
    TLogger Logger = ReporterLogger;

private:
    TSimpleCounter EnqueuedCounter_ = {"/enqueued"};
    TSimpleCounter DequeuedCounter_ = {"/dequeued"};
    TSimpleCounter DroppedCounter_ = {"/dropped"};
    TSimpleCounter PendingCounter_ = {"/pending"};
    TSimpleCounter CommittedCounter_ = {"/committed"};
    TSimpleCounter CommittedDataWeightCounter_ = {"/committed_data_weight"};

    const TSharedDataPtr Data_;
    const TStatisticsReporterConfigPtr Config_;
    const INativeClientPtr Client_;
    const TProfiler& Profiler_;
    TLimiter Limiter_;
    TNonblockingBatch<TJobStatistics> Batcher_;

    TAsyncSemaphore EnableSemaphore_ {1};
    std::atomic<bool> Enabled_ = {false};
    std::atomic<ui64> DroppedCount_ = {0};

    // Must return dataweight of written batch inside transaction.
    virtual size_t HandleBatchTransaction(ITransaction& transaction, const TBatch& batch) = 0;

    void Loop()
    {
        while (true) {
            WaitForEnabled();
            auto asyncBatch = Batcher_.DequeueBatch();
            auto batchOrError = WaitFor(asyncBatch);
            auto batch = batchOrError.ValueOrThrow();

            if (batch.empty()) {
                continue; // reporting has been disabled
            }

            Profiler_.Increment(PendingCounter_, -batch.size());
            Profiler_.Increment(DequeuedCounter_, batch.size());
            WriteBatchWithExpBackoff(batch);
        }
    }

    void WriteBatchWithExpBackoff(const TBatch& batch)
    {
        auto delay = Config_->MinRepeatDelay;
        while (IsEnabled()) {
            auto dropped = DroppedCount_.exchange(0);
            if (dropped) {
                LOG_WARNING("Maximum items reached, dropping job statistics (DroppedItems: %v)", dropped);
            }
            try {
                TryHandleBatch(batch);
                ui64 dataSize = 0;
                for (auto& stat : batch) {
                    dataSize += stat.EstimateSize();
                }
                Limiter_.Decrease(dataSize);
                return;
            } catch (const std::exception& ex) {
                LOG_WARNING(ex, "Failed to report job statistics (RetryDelay: %v, PendingItems: %v)",
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
        LOG_DEBUG("Job statistics transaction starting (Items: %v, PendingItems: %v, ArchiveVersion: %v)",
            batch.size(),
            GetPendingCount(),
            Data_->GetOperationArchiveVersion());
        TTransactionStartOptions transactionOptions;
        transactionOptions.Atomicity = Data_->GetOperationArchiveVersion() >= 16
            ? EAtomicity::None
            : EAtomicity::Full;
        auto asyncTransaction = Client_->StartTransaction(ETransactionType::Tablet, transactionOptions);
        auto transactionOrError = WaitFor(asyncTransaction);
        auto transaction = transactionOrError.ValueOrThrow();
        LOG_DEBUG("Job statistics transaction started (TransactionId: %v, Items: %v)",
            transaction->GetId(),
            batch.size());

        size_t dataWeight = HandleBatchTransaction(*transaction, batch);

        WaitFor(transaction->Commit())
            .ThrowOnError();

        Profiler_.Increment(CommittedCounter_, batch.size());
        Profiler_.Increment(CommittedDataWeightCounter_, dataWeight);

        LOG_DEBUG("Job statistics transaction committed (TransactionId: %v, "
            "CommittedItems: %v, CommittedDataWeight: %v)",
            transaction->GetId(),
            batch.size(),
            dataWeight);
    }

    ui64 GetPendingCount()
    {
        return PendingCounter_.GetCurrent();
    }

    void DoEnable()
    {
        EnableSemaphore_.Release();
        LOG_INFO("Job statistics reporter enabled");
    }

    void DoDisable()
    {
        EnableSemaphore_.Acquire();
        Batcher_.Drop();
        Limiter_.Reset();
        DroppedCount_.store(0, std::memory_order_relaxed);
        Profiler_.Update(PendingCounter_, 0);
        LOG_INFO("Job statistics reporter disabled");
    }

    bool IsEnabled()
    {
        return EnableSemaphore_.IsReady();
    }

    void WaitForEnabled()
    {
        if (IsEnabled()) {
            return;
        }
        LOG_INFO("Waiting for job statistics reporter to become enabled");
        auto event = EnableSemaphore_.GetReadyEvent();
        WaitFor(event).ThrowOnError();
        LOG_INFO("Job statistics reporter became enabled, resuming statistics writing");
    }
};

DECLARE_REFCOUNTED_TYPE(THandlerBase)
DEFINE_REFCOUNTED_TYPE(THandlerBase)

////////////////////////////////////////////////////////////////////////////////

class TJobHandler
    : public THandlerBase
{
public:
    TJobHandler(
        const TString& localAddress,
        TSharedDataPtr data,
        const TStatisticsReporterConfigPtr& config,
        INativeClientPtr client,
        IInvokerPtr invoker)
        : THandlerBase(
            std::move(data),
            config,
            "jobs",
            std::move(client),
            invoker,
            JobProfiler,
            config->MaxInProgressJobDataSize)
        , DefaultLocalAddress_(localAddress)
    { }

private:
    const TJobTableDescriptor Table_;
    const TString DefaultLocalAddress_;

    virtual size_t HandleBatchTransaction(ITransaction& transaction, const TBatch& batch) override
    {
        std::vector<TUnversionedRow> rows;
        auto rowBuffer = New<TRowBuffer>(TJobTag());

        size_t dataWeight = 0;
        for (auto&& statistics : batch) {
            TUnversionedRowBuilder builder;
            builder.AddValue(MakeUnversionedUint64Value(statistics.OperationId().Parts64[0], Table_.Ids.OperationIdHi));
            builder.AddValue(MakeUnversionedUint64Value(statistics.OperationId().Parts64[1], Table_.Ids.OperationIdLo));
            builder.AddValue(MakeUnversionedUint64Value(statistics.JobId().Parts64[0], Table_.Ids.JobIdHi));
            builder.AddValue(MakeUnversionedUint64Value(statistics.JobId().Parts64[1], Table_.Ids.JobIdLo));
            if (statistics.Type()) {
                builder.AddValue(MakeUnversionedStringValue(*statistics.Type(), Table_.Ids.Type));
            }
            if (statistics.State()) {
                builder.AddValue(MakeUnversionedStringValue(
                    *statistics.State(),
                    GetSharedData()->GetOperationArchiveVersion() >= 16
                        ? Table_.Ids.TransientState
                        : Table_.Ids.State));
            }
            if (statistics.StartTime()) {
                builder.AddValue(MakeUnversionedInt64Value(*statistics.StartTime(), Table_.Ids.StartTime));
            }
            if (statistics.FinishTime()) {
                builder.AddValue(MakeUnversionedInt64Value(*statistics.FinishTime(), Table_.Ids.FinishTime));
            }
            builder.AddValue(MakeUnversionedStringValue(DefaultLocalAddress_, Table_.Ids.Address));
            if (statistics.Error()) {
                builder.AddValue(MakeUnversionedAnyValue(*statistics.Error(), Table_.Ids.Error));
            }
            if (statistics.Statistics()) {
                builder.AddValue(MakeUnversionedAnyValue(*statistics.Statistics(), Table_.Ids.Statistics));
            }
            if (statistics.Events()) {
                builder.AddValue(MakeUnversionedAnyValue(*statistics.Events(), Table_.Ids.Events));
            }
            if (GetSharedData()->GetOperationArchiveVersion() >= 18) {
                builder.AddValue(MakeUnversionedInt64Value(TInstant::Now().MicroSeconds(), Table_.Ids.UpdateTime));
            }
            rows.push_back(rowBuffer->Capture(builder.GetRow()));
            dataWeight += GetDataWeight(rows.back());
        }

        transaction.WriteRows(
            GetOperationsArchiveJobsPath(),
            Table_.NameTable,
            MakeSharedRange(std::move(rows), std::move(rowBuffer)));

        return dataWeight;
    }
};

DECLARE_REFCOUNTED_TYPE(TJobHandler)
DEFINE_REFCOUNTED_TYPE(TJobHandler)

class TJobSpecHandler
    : public THandlerBase
{
public:
    TJobSpecHandler(
        TSharedDataPtr data,
        const TStatisticsReporterConfigPtr& config,
        INativeClientPtr client,
        IInvokerPtr invoker)
        : THandlerBase(
            std::move(data),
            config,
            "job_specs",
            std::move(client),
            invoker,
            JobSpecProfiler,
            config->MaxInProgressJobSpecDataSize)
    { }

private:
    const TJobSpecTableDescriptor Table_;

    virtual size_t HandleBatchTransaction(ITransaction& transaction, const TBatch& batch) override
    {
        std::vector<TUnversionedRow> rows;
        auto rowBuffer = New<TRowBuffer>(TJobSpecTag());

        size_t dataWeight = 0;
        for (auto&& statistics : batch) {
            TUnversionedRowBuilder builder;
            builder.AddValue(MakeUnversionedUint64Value(statistics.JobId().Parts64[0], Table_.Ids.JobIdHi));
            builder.AddValue(MakeUnversionedUint64Value(statistics.JobId().Parts64[1], Table_.Ids.JobIdLo));
            if (statistics.Spec()) {
                builder.AddValue(MakeUnversionedStringValue(*statistics.Spec(), Table_.Ids.Spec));
            }
            if (statistics.SpecVersion()) {
                builder.AddValue(MakeUnversionedInt64Value(*statistics.SpecVersion(), Table_.Ids.SpecVersion));
            }
            if (GetSharedData()->GetOperationArchiveVersion() >= 16) {
                if (statistics.Type()) {
                    builder.AddValue(MakeUnversionedStringValue(*statistics.Type(), Table_.Ids.Type));
                }
            }
            rows.push_back(rowBuffer->Capture(builder.GetRow()));
            dataWeight += GetDataWeight(rows.back());
        }

        transaction.WriteRows(
            GetOperationsArchiveJobSpecsPath(),
            Table_.NameTable,
            MakeSharedRange(std::move(rows), std::move(rowBuffer)));

        return dataWeight;
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TStatisticsReporter::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TStatisticsReporterConfigPtr reporterConfig,
        TBootstrap* bootstrap)
        : Client_(
            bootstrap->GetMasterConnection()->CreateNativeClient(TClientOptions(reporterConfig->User)))
        , JobHandler_(
            New<TJobHandler>(
                bootstrap->GetMasterConnector()->GetLocalDescriptor().GetDefaultAddress(),
                Data_,
                reporterConfig,
                Client_,
                Reporter_->GetInvoker()))
        , JobSpecHandler_(
            New<TJobSpecHandler>(
                Data_,
                reporterConfig,
                Client_,
                Reporter_->GetInvoker()))
    { }

    void ReportStatistics(TJobStatistics&& statistics)
    {
        if (IsSpecEntry(statistics)) {
            JobSpecHandler_->Enqueue(statistics.ExtractSpec());
        }
        if (!statistics.IsEmpty()) {
            JobHandler_->Enqueue(std::move(statistics));
        }
    }

    void SetEnabled(bool enable)
    {
        JobHandler_->SetEnabled(enable);
    }

    void SetSpecEnabled(bool enable)
    {
        JobSpecHandler_->SetEnabled(enable);
    }

    void SetOperationArchiveVersion(int version)
    {
        Data_->SetOperationArchiveVersion(version);
    }

private:
    const INativeClientPtr Client_;
    const TActionQueuePtr Reporter_ = New<TActionQueue>("Reporter");
    const TSharedDataPtr Data_ = New<TSharedData>();
    const TJobHandlerPtr JobHandler_;
    const THandlerBasePtr JobSpecHandler_;
};

////////////////////////////////////////////////////////////////////////////////

TStatisticsReporter::TStatisticsReporter(
    TStatisticsReporterConfigPtr reporterConfig,
    TBootstrap* bootstrap)
    : Impl_(
        reporterConfig->Enabled
            ? New<TImpl>(std::move(reporterConfig), bootstrap)
            : nullptr)
{ }

void TStatisticsReporter::ReportStatistics(TJobStatistics&& statistics)
{
    if (Impl_) {
        Impl_->ReportStatistics(std::move(statistics));
    }
}

void TStatisticsReporter::SetEnabled(bool enable)
{
    if (Impl_) {
        Impl_->SetEnabled(enable);
    }
}

void TStatisticsReporter::SetSpecEnabled(bool enable)
{
    if (Impl_) {
        Impl_->SetSpecEnabled(enable);
    }
}

void TStatisticsReporter::SetOperationArchiveVersion(int version)
{
    if (Impl_) {
        Impl_->SetOperationArchiveVersion(version);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobAgent
} // namespace NYT
