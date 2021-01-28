#include "job_reporter.h"

#include "config.h"
#include "job_report.h"

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/controller_agent/helpers.h>

#include <yt/ytlib/scheduler/helpers.h>

#include <yt/client/api/connection.h>
#include <yt/client/api/transaction.h>

#include <yt/client/tablet_client/table_mount_cache.h>

#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/name_table.h>

#include <yt/core/compression/codec.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/nonblocking_batch.h>
#include <yt/core/concurrency/async_semaphore.h>

#include <yt/core/utilex/random.h>

namespace NYT::NJobAgent {

using namespace NNodeTrackerClient;
using namespace NTransactionClient;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;
using namespace NControllerAgent;
using namespace NApi;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NProfiling;
using namespace NScheduler;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

static constexpr int QueueIsTooLargeMultiplier = 2;

struct TJobTag
{ };

struct TOperationIdTag
{ };

struct TJobSpecTag
{ };

struct TJobStderrTag
{ };

struct TJobProfileTag
{ };

struct TJobFailContextTag
{ };

namespace {

static const TRegistry ReporterProfiler("/job_reporter");
static const TLogger ReporterLogger("JobReporter");

////////////////////////////////////////////////////////////////////////////////

bool IsSpecEntry(const TJobReport& stat)
{
    return stat.Spec().operator bool();
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

    ui64 GetValue() const
    {
        return Value_.load();
    }

    ui64 GetMaxValue()
    {
        return MaxValue_;
    }

private:
    const ui64 MaxValue_;
    std::atomic<ui64> Value_ = {0};
};

////////////////////////////////////////////////////////////////////////////////

using TBatch = std::vector<TJobReport>;

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
        TJobReporterConfigPtr config,
        TString reporterName,
        NNative::IClientPtr client,
        IInvokerPtr invoker,
        const TRegistry& profiler,
        ui64 maxInProgressDataSize)
        : Config_(std::move(config))
        , ReporterName_(std::move(reporterName))
        , Data_(std::move(data))
        , Client_(std::move(client))
        , Limiter_(maxInProgressDataSize)
        , Batcher_(New<TNonblockingBatch<TJobReport>>(Config_->MaxItemsInBatch, Config_->ReportingPeriod))
        , EnqueuedCounter_(profiler.Counter("/enqueued"))
        , DequeuedCounter_(profiler.Counter("/dequeued"))
        , DroppedCounter_(profiler.Counter("/dropped"))
        , WriteFailuresCounter_(profiler.Counter("/write_failures"))
        , PendingCounter_(profiler.Gauge("/pending"))
        , QueueIsTooLargeCounter_(profiler.Gauge("/queue_is_too_large"))
        , CommittedCounter_(profiler.Counter("/committed"))
        , CommittedDataWeightCounter_(profiler.Counter("/committed_data_weight"))
    {
        BIND(&THandlerBase::Loop, MakeWeak(this))
            .Via(invoker)
            .Run();
        EnableSemaphore_->Acquire();
        Logger.AddTag("Reporter: %v", ReporterName_);
    }

    void Enqueue(TJobReport&& statistics)
    {
        if (!IsEnabled()) {
            return;
        }
        if (Limiter_.TryIncrease(statistics.EstimateSize())) {
            Batcher_->Enqueue(std::move(statistics));
            PendingCounter_.Update(PendingCount_++);
            EnqueuedCounter_.Increment();
            QueueIsTooLargeCounter_.Update(
                QueueIsTooLargeMultiplier * Limiter_.GetValue() > Limiter_.GetMaxValue()
                ? 1
                : 0);
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

    const TSharedDataPtr& GetSharedData()
    {
        return Data_;
    }

    int ExtractWriteFailuresCount()
    {
        return WriteFailuresCount_.exchange(0);
    }

    bool QueueIsTooLarge()
    {
        return QueueIsTooLargeMultiplier * Limiter_.GetValue() > Limiter_.GetMaxValue();
    }

protected:
    TLogger Logger = ReporterLogger;
    const TJobReporterConfigPtr Config_;

    bool IsValueWeightViolated(TUnversionedRow row, TOperationId operationId, TJobId jobId, const TNameTablePtr& nameTable)
    {
        for (auto value : row) {
            auto valueWeight = GetDataWeight(value);
            if (valueWeight > MaxStringValueLength) {
                YT_LOG_WARNING(
                    "Job report row violates value data weight, archivation skipped"
                    "(ReporterName: %v, OperationId: %v, JobId: %v, Key: %v, Weight: %v, WeightLimit: %v)",
                    ReporterName_,
                    operationId,
                    jobId,
                    nameTable->GetNameOrThrow(value.Id),
                    valueWeight,
                    MaxStringValueLength);
                return true;
            }
        }
        return false;
    };

private:
    const TString ReporterName_;
    const TSharedDataPtr Data_;
    const NNative::IClientPtr Client_;
    TLimiter Limiter_;
    TNonblockingBatchPtr<TJobReport> Batcher_;

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

    // Must return dataweight of written batch inside transaction.
    virtual size_t HandleBatchTransaction(ITransaction& transaction, const TBatch& batch) = 0;

    void Loop()
    {
        while (true) {
            WaitForEnabled();
            auto asyncBatch = Batcher_->DequeueBatch();
            auto batchOrError = WaitFor(asyncBatch);
            auto batch = batchOrError.ValueOrThrow();

            if (batch.empty()) {
                continue; // reporting has been disabled
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
                YT_LOG_WARNING("Maximum items reached, dropping job statistics (DroppedItems: %v)", dropped);
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
                WriteFailuresCount_.fetch_add(1, std::memory_order_relaxed);
                WriteFailuresCounter_.Increment();
                YT_LOG_WARNING(ex, "Failed to report job statistics (RetryDelay: %v, PendingItems: %v)",
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
        YT_LOG_DEBUG("Job statistics transaction starting (Items: %v, PendingItems: %v, ArchiveVersion: %v)",
            batch.size(),
            GetPendingCount(),
            Data_->GetOperationArchiveVersion());
        TTransactionStartOptions transactionOptions;
        transactionOptions.Atomicity = EAtomicity::None;
        auto asyncTransaction = Client_->StartTransaction(ETransactionType::Tablet, transactionOptions);
        auto transactionOrError = WaitFor(asyncTransaction);
        auto transaction = transactionOrError.ValueOrThrow();
        YT_LOG_DEBUG("Job statistics transaction started (TransactionId: %v, Items: %v)",
            transaction->GetId(),
            batch.size());

        size_t dataWeight = HandleBatchTransaction(*transaction, batch);

        WaitFor(transaction->Commit())
            .ThrowOnError();

        CommittedCounter_.Increment();
        CommittedDataWeightCounter_.Increment(dataWeight);

        YT_LOG_DEBUG("Job statistics transaction committed (TransactionId: %v, "
            "CommittedItems: %v, CommittedDataWeight: %v)",
            transaction->GetId(),
            batch.size(),
            dataWeight);
    }

    ui64 GetPendingCount()
    {
        return PendingCount_.load();
    }

    void DoEnable()
    {
        EnableSemaphore_->Release();
        YT_LOG_INFO("Job statistics reporter enabled");
    }

    void DoDisable()
    {
        EnableSemaphore_->Acquire();
        Batcher_->Drop();
        Limiter_.Reset();
        DroppedCount_.store(0, std::memory_order_relaxed);
        PendingCounter_.Update(PendingCount_ = 0);
        QueueIsTooLargeCounter_.Update(0);
        YT_LOG_INFO("Job statistics reporter disabled");
    }

    bool IsEnabled()
    {
        return EnableSemaphore_->IsReady();
    }

    void WaitForEnabled()
    {
        if (IsEnabled()) {
            return;
        }
        YT_LOG_INFO("Waiting for job statistics reporter to become enabled");
        auto event = EnableSemaphore_->GetReadyEvent();
        WaitFor(event).ThrowOnError();
        YT_LOG_INFO("Job statistics reporter became enabled, resuming statistics writing");
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
        std::optional<TString> localAddress,
        TSharedDataPtr data,
        const TJobReporterConfigPtr& config,
        NNative::IClientPtr client,
        const IInvokerPtr& invoker)
        : THandlerBase(
            std::move(data),
            config,
            "jobs",
            std::move(client),
            invoker,
            ReporterProfiler.WithTag("reporter_type", "jobs"),
            config->MaxInProgressJobDataSize)
        , DefaultLocalAddress_(std::move(localAddress))
    { }

private:
    const TJobTableDescriptor Table_;
    const std::optional<TString> DefaultLocalAddress_;

    virtual size_t HandleBatchTransaction(ITransaction& transaction, const TBatch& batch) override
    {
        std::vector<TUnversionedRow> rows;
        auto rowBuffer = New<TRowBuffer>(TJobTag());

        size_t dataWeight = 0;
        for (auto&& statistics : batch) {
            // It must be alive until row capture.
            TYsonString coreInfosYsonString;
            TString jobCompetitionIdString;
            TString statisticsLz4;
            TYsonString briefStatisticsYsonString;

            TUnversionedRowBuilder builder;
            builder.AddValue(MakeUnversionedUint64Value(statistics.OperationId().Parts64[0], Table_.Index.OperationIdHi));
            builder.AddValue(MakeUnversionedUint64Value(statistics.OperationId().Parts64[1], Table_.Index.OperationIdLo));
            builder.AddValue(MakeUnversionedUint64Value(statistics.JobId().Parts64[0], Table_.Index.JobIdHi));
            builder.AddValue(MakeUnversionedUint64Value(statistics.JobId().Parts64[1], Table_.Index.JobIdLo));
            if (statistics.Type()) {
                builder.AddValue(MakeUnversionedStringValue(*statistics.Type(), Table_.Index.Type));
            }
            if (statistics.State()) {
                builder.AddValue(MakeUnversionedStringValue(
                    *statistics.State(),
                    Table_.Index.TransientState));
            }
            if (statistics.StartTime()) {
                builder.AddValue(MakeUnversionedInt64Value(*statistics.StartTime(), Table_.Index.StartTime));
            }
            if (statistics.FinishTime()) {
                builder.AddValue(MakeUnversionedInt64Value(*statistics.FinishTime(), Table_.Index.FinishTime));
            }
            if (DefaultLocalAddress_) {
                builder.AddValue(MakeUnversionedStringValue(*DefaultLocalAddress_, Table_.Index.Address));
            }
            if (statistics.Error()) {
                builder.AddValue(MakeUnversionedAnyValue(*statistics.Error(), Table_.Index.Error));
            }
            if (statistics.Statistics()) {
                constexpr int Lz4AndBriefStatisticsVersion = 36;
                if (Config_->ReportStatisticsLz4 && GetSharedData()->GetOperationArchiveVersion() >= Lz4AndBriefStatisticsVersion) {
                    auto codec = NCompression::GetCodec(NCompression::ECodec::Lz4);
                    statisticsLz4 = ToString(codec->Compress(TSharedRef::FromString(*statistics.Statistics())));
                    builder.AddValue(MakeUnversionedStringValue(statisticsLz4, Table_.Index.StatisticsLz4));
                } else {
                    builder.AddValue(MakeUnversionedAnyValue(*statistics.Statistics(), Table_.Index.Statistics));
                }
                if (GetSharedData()->GetOperationArchiveVersion() >= Lz4AndBriefStatisticsVersion) {
                    briefStatisticsYsonString = BuildBriefStatistics(ConvertToNode(TYsonStringBuf(*statistics.Statistics())));
                    builder.AddValue(MakeUnversionedAnyValue(briefStatisticsYsonString.AsStringBuf(), Table_.Index.BriefStatistics));
                }
            }
            if (statistics.Events()) {
                builder.AddValue(MakeUnversionedAnyValue(*statistics.Events(), Table_.Index.Events));
            }
            if (statistics.StderrSize()) {
                builder.AddValue(MakeUnversionedUint64Value(*statistics.StderrSize(), Table_.Index.StderrSize));
            }
            if (GetSharedData()->GetOperationArchiveVersion() >= 31 && statistics.CoreInfos()) {
                coreInfosYsonString = ConvertToYsonString(*statistics.CoreInfos());
                builder.AddValue(MakeUnversionedAnyValue(coreInfosYsonString.AsStringBuf(), Table_.Index.CoreInfos));
            }
            if (GetSharedData()->GetOperationArchiveVersion() >= 18) {
                builder.AddValue(MakeUnversionedInt64Value(TInstant::Now().MicroSeconds(), Table_.Index.UpdateTime));
            }
            if (GetSharedData()->GetOperationArchiveVersion() >= 20 && statistics.Spec()) {
                builder.AddValue(MakeUnversionedBooleanValue(statistics.Spec().operator bool(), Table_.Index.HasSpec));
            }
            if (statistics.FailContext()) {
                if (GetSharedData()->GetOperationArchiveVersion() >= 23) {
                    builder.AddValue(MakeUnversionedUint64Value(statistics.FailContext()->size(), Table_.Index.FailContextSize));
                } else if (GetSharedData()->GetOperationArchiveVersion() >= 21) {
                    builder.AddValue(MakeUnversionedBooleanValue(statistics.FailContext().operator bool(), Table_.Index.HasFailContext));
                }
            }
            if (GetSharedData()->GetOperationArchiveVersion() >= 32 && statistics.JobCompetitionId()) {
                jobCompetitionIdString = ToString(statistics.JobCompetitionId());
                builder.AddValue(MakeUnversionedStringValue(jobCompetitionIdString, Table_.Index.JobCompetitionId));
            }
            if (GetSharedData()->GetOperationArchiveVersion() >= 33 && statistics.HasCompetitors().has_value()) {
                builder.AddValue(MakeUnversionedBooleanValue(statistics.HasCompetitors().value(), Table_.Index.HasCompetitors));
            }
            // COMPAT(gritukan)
            if (GetSharedData()->GetOperationArchiveVersion() >= 34 && statistics.ExecAttributes()) {
                builder.AddValue(MakeUnversionedAnyValue(*statistics.ExecAttributes(), Table_.Index.ExecAttributes));
            }
            // COMPAT(gritukan)
            if (GetSharedData()->GetOperationArchiveVersion() >= 35 && statistics.TaskName()) {
                builder.AddValue(MakeUnversionedStringValue(*statistics.TaskName(), Table_.Index.TaskName));
            }
            // COMPAT(levysotsky)
            if (GetSharedData()->GetOperationArchiveVersion() >= 37 && statistics.TreeId()) {
                builder.AddValue(MakeUnversionedStringValue(*statistics.TreeId(), Table_.Index.PoolTree));
            }
            // COMPAT(levysotsky)
            if (GetSharedData()->GetOperationArchiveVersion() >= 39 && statistics.MonitoringDescriptor()) {
                builder.AddValue(MakeUnversionedStringValue(*statistics.MonitoringDescriptor(), Table_.Index.MonitoringDescriptor));
            }

            if (!IsValueWeightViolated(builder.GetRow(), statistics.OperationId(), statistics.JobId(), Table_.NameTable)) {
                rows.push_back(rowBuffer->Capture(builder.GetRow()));
                dataWeight += GetDataWeight(rows.back());
            }
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

class TOperationIdHandler
    : public THandlerBase
{
public:
    TOperationIdHandler(
        TSharedDataPtr data,
        const TJobReporterConfigPtr& config,
        NNative::IClientPtr client,
        const IInvokerPtr& invoker)
        : THandlerBase(
            std::move(data),
            config,
            "operation_ids",
            std::move(client),
            invoker,
            ReporterProfiler.WithTag("reporter_type", "operation_ids"),
            config->MaxInProgressOperationIdDataSize)
    { }

private:
    const TOperationIdTableDescriptor Table_;

    virtual size_t HandleBatchTransaction(ITransaction& transaction, const TBatch& batch) override
    {
        std::vector<TUnversionedRow> rows;
        auto rowBuffer = New<TRowBuffer>(TOperationIdTag());

        size_t dataWeight = 0;
        for (auto&& statistics : batch) {
            TUnversionedRowBuilder builder;
            builder.AddValue(MakeUnversionedUint64Value(statistics.JobId().Parts64[0], Table_.Index.JobIdHi));
            builder.AddValue(MakeUnversionedUint64Value(statistics.JobId().Parts64[1], Table_.Index.JobIdLo));
            builder.AddValue(MakeUnversionedUint64Value(statistics.OperationId().Parts64[0], Table_.Index.OperationIdHi));
            builder.AddValue(MakeUnversionedUint64Value(statistics.OperationId().Parts64[1], Table_.Index.OperationIdLo));

            if (!IsValueWeightViolated(builder.GetRow(), statistics.OperationId(), statistics.JobId(), Table_.NameTable)) {
                rows.push_back(rowBuffer->Capture(builder.GetRow()));
                dataWeight += GetDataWeight(rows.back());
            }
        }

        transaction.WriteRows(
            GetOperationsArchiveOperationIdsPath(),
            Table_.NameTable,
            MakeSharedRange(std::move(rows), std::move(rowBuffer)));

        return dataWeight;
    }
};

DECLARE_REFCOUNTED_TYPE(TOperationIdHandler)
DEFINE_REFCOUNTED_TYPE(TOperationIdHandler)

class TJobSpecHandler
    : public THandlerBase
{
public:
    TJobSpecHandler(
        TSharedDataPtr data,
        const TJobReporterConfigPtr& config,
        NNative::IClientPtr client,
        IInvokerPtr invoker)
        : THandlerBase(
            std::move(data),
            config,
            "job_specs",
            std::move(client),
            invoker,
            ReporterProfiler.WithTag("reporter_type", "job_specs"),
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
            builder.AddValue(MakeUnversionedUint64Value(statistics.JobId().Parts64[0], Table_.Index.JobIdHi));
            builder.AddValue(MakeUnversionedUint64Value(statistics.JobId().Parts64[1], Table_.Index.JobIdLo));
            if (statistics.Spec()) {
                builder.AddValue(MakeUnversionedStringValue(*statistics.Spec(), Table_.Index.Spec));
            }
            if (statistics.SpecVersion()) {
                builder.AddValue(MakeUnversionedInt64Value(*statistics.SpecVersion(), Table_.Index.SpecVersion));
            }
            if (statistics.Type()) {
                builder.AddValue(MakeUnversionedStringValue(*statistics.Type(), Table_.Index.Type));
            }

            if (!IsValueWeightViolated(builder.GetRow(), statistics.OperationId(), statistics.JobId(), Table_.NameTable)) {
                rows.push_back(rowBuffer->Capture(builder.GetRow()));
                dataWeight += GetDataWeight(rows.back());
            }
        }

        transaction.WriteRows(
            GetOperationsArchiveJobSpecsPath(),
            Table_.NameTable,
            MakeSharedRange(std::move(rows), std::move(rowBuffer)));

        return dataWeight;
    }
};

class TJobStderrHandler
    : public THandlerBase
{
public:
    TJobStderrHandler(
        TSharedDataPtr data,
        const TJobReporterConfigPtr& config,
        NNative::IClientPtr client,
        IInvokerPtr invoker)
        : THandlerBase(
            std::move(data),
            config,
            "stderrs",
            std::move(client),
            invoker,
            ReporterProfiler.WithTag("reporter_type", "stderrs"),
            config->MaxInProgressJobStderrDataSize)
    { }

private:
    const TJobStderrTableDescriptor Table_;

    virtual size_t HandleBatchTransaction(ITransaction& transaction, const TBatch& batch) override
    {
        std::vector<TUnversionedRow> rows;
        auto rowBuffer = New<TRowBuffer>(TJobProfileTag());

        size_t dataWeight = 0;
        for (auto&& statistics : batch) {
            if (statistics.Stderr()) {
                TUnversionedRowBuilder builder;
                builder.AddValue(MakeUnversionedUint64Value(statistics.OperationId().Parts64[0], Table_.Index.OperationIdHi));
                builder.AddValue(MakeUnversionedUint64Value(statistics.OperationId().Parts64[1], Table_.Index.OperationIdLo));
                builder.AddValue(MakeUnversionedUint64Value(statistics.JobId().Parts64[0], Table_.Index.JobIdHi));
                builder.AddValue(MakeUnversionedUint64Value(statistics.JobId().Parts64[1], Table_.Index.JobIdLo));
                builder.AddValue(MakeUnversionedStringValue(*statistics.Stderr(), Table_.Index.Stderr));

                if (!IsValueWeightViolated(builder.GetRow(), statistics.OperationId(), statistics.JobId(), Table_.NameTable)) {
                    rows.push_back(rowBuffer->Capture(builder.GetRow()));
                    dataWeight += GetDataWeight(rows.back());
                }
            }
        }

        transaction.WriteRows(
            GetOperationsArchiveJobStderrsPath(),
            Table_.NameTable,
            MakeSharedRange(std::move(rows), std::move(rowBuffer)));

        return dataWeight;
    }
};

class TJobProfileHandler
    : public THandlerBase
{
public:
    TJobProfileHandler(
        TSharedDataPtr data,
        const TJobReporterConfigPtr& config,
        NNative::IClientPtr client,
        IInvokerPtr invoker)
        : THandlerBase(
            std::move(data),
            config,
            "profiles",
            std::move(client),
            invoker,
            ReporterProfiler.WithTag("reporter_type", "profiles"),
            config->MaxInProgressJobStderrDataSize)
    { }

private:
    const TJobProfileTableDescriptor Table_;

    virtual size_t HandleBatchTransaction(ITransaction& transaction, const TBatch& batch) override
    {
        if (GetSharedData()->GetOperationArchiveVersion() < 27) {
            return 0;
        }

        std::vector<TUnversionedRow> rows;
        auto rowBuffer = New<TRowBuffer>(TJobStderrTag());

        size_t dataWeight = 0;
        for (auto&& statistics : batch) {
            auto profile = statistics.Profile();
            if (profile) {
                TUnversionedRowBuilder builder;
                builder.AddValue(MakeUnversionedUint64Value(statistics.OperationId().Parts64[0], Table_.Index.OperationIdHi));
                builder.AddValue(MakeUnversionedUint64Value(statistics.OperationId().Parts64[1], Table_.Index.OperationIdLo));
                builder.AddValue(MakeUnversionedUint64Value(statistics.JobId().Parts64[0], Table_.Index.JobIdHi));
                builder.AddValue(MakeUnversionedUint64Value(statistics.JobId().Parts64[1], Table_.Index.JobIdLo));
                builder.AddValue(MakeUnversionedInt64Value(0, Table_.Index.PartIndex));
                builder.AddValue(MakeUnversionedStringValue(profile->Type, Table_.Index.ProfileType));
                builder.AddValue(MakeUnversionedStringValue(profile->Blob, Table_.Index.ProfileBlob));

                if (!IsValueWeightViolated(builder.GetRow(), statistics.OperationId(), statistics.JobId(), Table_.NameTable)) {
                    rows.push_back(rowBuffer->Capture(builder.GetRow()));
                    dataWeight += GetDataWeight(rows.back());
                }
            }
        }

        transaction.WriteRows(
            GetOperationsArchiveJobProfilesPath(),
            Table_.NameTable,
            MakeSharedRange(std::move(rows), std::move(rowBuffer)));

        return dataWeight;
    }
};

class TJobFailContextHandler
    : public THandlerBase
{
public:
    TJobFailContextHandler(
        TSharedDataPtr data,
        const TJobReporterConfigPtr& config,
        NNative::IClientPtr client,
        IInvokerPtr invoker)
        : THandlerBase(
            std::move(data),
            config,
            "fail_contexts",
            std::move(client),
            invoker,
            ReporterProfiler.WithTag("reporter_type", "fail_contexts"),
            config->MaxInProgressJobFailContextDataSize)
    { }

private:
    const TJobFailContextTableDescriptor Table_;

    virtual size_t HandleBatchTransaction(ITransaction& transaction, const TBatch& batch) override
    {
        if (GetSharedData()->GetOperationArchiveVersion() < 21) {
            return 0;
        }

        std::vector<TUnversionedRow> rows;
        auto rowBuffer = New<TRowBuffer>(TJobFailContextTag());

        size_t dataWeight = 0;
        for (auto&& statistics : batch) {
            if (statistics.FailContext() && statistics.FailContext()->size() <= MaxStringValueLength) {
                TUnversionedRowBuilder builder;
                builder.AddValue(MakeUnversionedUint64Value(statistics.OperationId().Parts64[0], Table_.Index.OperationIdHi));
                builder.AddValue(MakeUnversionedUint64Value(statistics.OperationId().Parts64[1], Table_.Index.OperationIdLo));
                builder.AddValue(MakeUnversionedUint64Value(statistics.JobId().Parts64[0], Table_.Index.JobIdHi));
                builder.AddValue(MakeUnversionedUint64Value(statistics.JobId().Parts64[1], Table_.Index.JobIdLo));
                builder.AddValue(MakeUnversionedStringValue(*statistics.FailContext(), Table_.Index.FailContext));

                if (!IsValueWeightViolated(builder.GetRow(), statistics.OperationId(), statistics.JobId(), Table_.NameTable)) {
                    rows.push_back(rowBuffer->Capture(builder.GetRow()));
                    dataWeight += GetDataWeight(rows.back());
                }
            }
        }

        transaction.WriteRows(
            GetOperationsArchiveJobFailContextsPath(),
            Table_.NameTable,
            MakeSharedRange(std::move(rows), std::move(rowBuffer)));

        return dataWeight;
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TJobReporter::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TJobReporterConfigPtr reporterConfig,
        const NApi::NNative::IConnectionPtr& masterConnection,
        std::optional<TString> localAddress)
        : Client_(
            masterConnection->CreateNativeClient(TClientOptions::FromUser(reporterConfig->User)))
        , JobHandler_(
            New<TJobHandler>(
                std::move(localAddress),
                Data_,
                reporterConfig,
                Client_,
                Reporter_->GetInvoker()))
        , OperationIdHandler_(
            New<TOperationIdHandler>(
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
        , JobStderrHandler_(
            New<TJobStderrHandler>(
                Data_,
                reporterConfig,
                Client_,
                Reporter_->GetInvoker()))
        , JobFailContextHandler_(
            New<TJobFailContextHandler>(
                Data_,
                reporterConfig,
                Client_,
                Reporter_->GetInvoker()))
        , JobProfileHandler_(
            New<TJobProfileHandler>(
                Data_,
                reporterConfig,
                Client_,
                Reporter_->GetInvoker()))
    { }

    void ReportStatistics(TJobReport&& statistics)
    {
        if (IsSpecEntry(statistics)) {
            JobSpecHandler_->Enqueue(statistics.ExtractSpec());
        }
        if (statistics.Stderr()) {
            JobStderrHandler_->Enqueue(statistics.ExtractStderr());
        }
        if (statistics.FailContext()) {
            JobFailContextHandler_->Enqueue(statistics.ExtractFailContext());
        }
        if (statistics.Profile()) {
            JobProfileHandler_->Enqueue(statistics.ExtractProfile());
        }
        if (!statistics.IsEmpty()) {
            OperationIdHandler_->Enqueue(statistics.ExtractIds());
            JobHandler_->Enqueue(std::move(statistics));
        }
    }

    void SetEnabled(bool enable)
    {
        JobHandler_->SetEnabled(enable);
        OperationIdHandler_->SetEnabled(enable);
    }

    void SetSpecEnabled(bool enable)
    {
        JobSpecHandler_->SetEnabled(enable);
    }

    void SetStderrEnabled(bool enable)
    {
        JobStderrHandler_->SetEnabled(enable);
    }

    void SetProfileEnabled(bool enable)
    {
        JobProfileHandler_->SetEnabled(enable);
    }

    void SetFailContextEnabled(bool enable)
    {
        JobFailContextHandler_->SetEnabled(enable);
    }

    void SetJobProfileEnabled(bool enable)
    {
        JobProfileHandler_->SetEnabled(enable);
    }

    void SetOperationArchiveVersion(int version)
    {
        Data_->SetOperationArchiveVersion(version);
    }

    int ExtractWriteFailuresCount()
    {
        return
            JobHandler_->ExtractWriteFailuresCount() +
            JobSpecHandler_->ExtractWriteFailuresCount() +
            JobStderrHandler_->ExtractWriteFailuresCount() +
            JobFailContextHandler_->ExtractWriteFailuresCount() +
            JobProfileHandler_->ExtractWriteFailuresCount();
    }

    bool GetQueueIsTooLarge()
    {
        return
            JobHandler_->QueueIsTooLarge() ||
            JobSpecHandler_->QueueIsTooLarge() ||
            JobStderrHandler_->QueueIsTooLarge() ||
            JobFailContextHandler_->QueueIsTooLarge() ||
            JobProfileHandler_->QueueIsTooLarge();
    }

private:
    const NNative::IClientPtr Client_;
    const TActionQueuePtr Reporter_ = New<TActionQueue>("JobReporter");
    const TSharedDataPtr Data_ = New<TSharedData>();
    const TJobHandlerPtr JobHandler_;
    const THandlerBasePtr OperationIdHandler_;
    const THandlerBasePtr JobSpecHandler_;
    const THandlerBasePtr JobStderrHandler_;
    const THandlerBasePtr JobFailContextHandler_;
    const THandlerBasePtr JobProfileHandler_;
};

////////////////////////////////////////////////////////////////////////////////

TJobReporter::TJobReporter(
    TJobReporterConfigPtr reporterConfig,
    const NApi::NNative::IConnectionPtr& masterConnection,
    std::optional<TString> localAddress)
    : Impl_(
        reporterConfig->Enabled
            ? New<TImpl>(std::move(reporterConfig), masterConnection, std::move(localAddress))
            : nullptr)
{ }

TJobReporter::~TJobReporter()
{ }

void TJobReporter::ReportStatistics(TJobReport&& statistics)
{
    if (Impl_) {
        Impl_->ReportStatistics(std::move(statistics));
    }
}

void TJobReporter::SetEnabled(bool enable)
{
    if (Impl_) {
        Impl_->SetEnabled(enable);
    }
}

void TJobReporter::SetSpecEnabled(bool enable)
{
    if (Impl_) {
        Impl_->SetSpecEnabled(enable);
    }
}

void TJobReporter::SetStderrEnabled(bool enable)
{
    if (Impl_) {
        Impl_->SetStderrEnabled(enable);
    }
}

void TJobReporter::SetProfileEnabled(bool enable)
{
    if (Impl_) {
        Impl_->SetProfileEnabled(enable);
    }
}

void TJobReporter::SetFailContextEnabled(bool enable)
{
    if (Impl_) {
        Impl_->SetFailContextEnabled(enable);
    }
}

void TJobReporter::SetOperationArchiveVersion(int version)
{
    if (Impl_) {
        Impl_->SetOperationArchiveVersion(version);
    }
}

int TJobReporter::ExtractWriteFailuresCount()
{
    if (Impl_) {
        return Impl_->ExtractWriteFailuresCount();
    }
    return 0;
}

bool TJobReporter::GetQueueIsTooLarge()
{
    if (Impl_) {
        return Impl_->GetQueueIsTooLarge();
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
