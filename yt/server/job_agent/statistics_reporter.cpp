#include "statistics_reporter.h"
#include "private.h"
#include "config.h"

#include <yt/server/data_node/master_connector.h>

#include <yt/server/cell_node/bootstrap.h>

#include <yt/ytlib/api/connection.h>
#include <yt/ytlib/api/native_client.h>
#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/tablet_client/table_mount_cache.h>

#include <yt/ytlib/table_client/row_buffer.h>
#include <yt/ytlib/table_client/name_table.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/nonblocking_batch.h>

#include <yt/core/profiling/profiler.h>

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

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobTrackerServerLogger;
static const TProfiler StatisticsProfiler("/statistics_reporter");

////////////////////////////////////////////////////////////////////////////////

struct TStatisticsTableDescriptor
{
    TStatisticsTableDescriptor()
        : NameTable(New<TNameTable>())
        , Ids(NameTable)
    { }

    struct TIndex
    {
        explicit TIndex(const TNameTablePtr& n)
            : OperationIdHi(n->RegisterName("operation_id_hi"))
            , OperationIdLo(n->RegisterName("operation_id_lo"))
            , JobIdHi(n->RegisterName("job_id_hi"))
            , JobIdLo(n->RegisterName("job_id_lo"))
            , Type(n->RegisterName("type"))
            , State(n->RegisterName("state"))
            , StartTime(n->RegisterName("start_time"))
            , FinishTime(n->RegisterName("finish_time"))
            , Address(n->RegisterName("address"))
            , Error(n->RegisterName("error"))
            , Spec(n->RegisterName("spec"))
            , SpecVersion(n->RegisterName("spec_version"))
            , Statistics(n->RegisterName("statistics"))
            , Events(n->RegisterName("events"))
        { }

        const int OperationIdHi;
        const int OperationIdLo;
        const int JobIdHi;
        const int JobIdLo;
        const int Type;
        const int State;
        const int StartTime;
        const int FinishTime;
        const int Address;
        const int Error;
        const int Spec;
        const int SpecVersion;
        const int Statistics;
        const int Events;
    };

    const TNameTablePtr NameTable;
    const TIndex Ids;
};

////////////////////////////////////////////////////////////////////////////////

struct TJobStatisticsTag
{ };

class TStatisticsReporter::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TStatisticsReporterConfigPtr reporterConfig,
        TBootstrap* bootstrap)
        : Config_(std::move(reporterConfig))
        , Client_(bootstrap->GetMasterClient())
        , DefaultLocalAddress_(bootstrap->GetMasterConnector()->GetLocalDescriptor().GetDefaultAddress())
        , NormalPriorityCounter_(Config_->MaxItemsInProgressNormalPriority)
        , LowPriorityCounter_(Config_->MaxItemsInProgressLowPriority)
        , Batcher_(New<TNonblockingBatch<TJobStatistics>>(Config_->MaxItemsInBatch, Config_->ReportingPeriod))
    {
        Reporter_->GetInvoker()->Invoke(BIND(&TImpl::OnReporting, MakeWeak(this)));
    }

    void ReportStatistics(TJobStatistics&& statistics)
    {
        auto& counter = GetCounter(statistics.Priority());
        if (++counter.InProgressCount > counter.MaxInProgressCount) {
            ++DroppedCount_;
            --counter.InProgressCount;
            StatisticsProfiler.Increment(DroppedCounter_);
        } else {
            Batcher_->Enqueue(std::move(statistics));
            StatisticsProfiler.Increment(EnqueuedCounter_);
        }
    }

private:
    using TStatisticsBatch = std::vector<TJobStatistics>;

    struct TPriorityCounter
    {
        TPriorityCounter(int maxInProgressCount)
            : MaxInProgressCount(maxInProgressCount)
        { }

        const int MaxInProgressCount;
        std::atomic<int> InProgressCount = {0};
    };

    const TStatisticsReporterConfigPtr Config_;
    const INativeClientPtr Client_;
    const TActionQueuePtr Reporter_ = New<TActionQueue>("Reporter");
    const Stroka DefaultLocalAddress_;
    const TStatisticsTableDescriptor Table_;

    TPriorityCounter NormalPriorityCounter_;
    TPriorityCounter LowPriorityCounter_;
    std::atomic<int> DroppedCount_ = {0};
    const TNonblockingBatchPtr<TJobStatistics> Batcher_;

    TSimpleCounter EnqueuedCounter_ = {"/enqueued"};
    TSimpleCounter DequeuedCounter_ = {"/dequeued"};
    TSimpleCounter DroppedCounter_ = {"/dropped"};
    TSimpleCounter CommittedCounter_ = {"/committed"};
    TSimpleCounter CommittedDataWeightCounter_ = {"/committed_data_weight"};

    TPriorityCounter& GetCounter(EReportPriority priority)
    {
        return priority == EReportPriority::Normal
            ? NormalPriorityCounter_
            : LowPriorityCounter_;
    }

    void OnReporting()
    {
        while (true) {
            auto asyncBatch = Batcher_->DequeueBatch();
            auto batchOrError = WaitFor(asyncBatch);
            auto batch = batchOrError.ValueOrThrow();

            StatisticsProfiler.Increment(DequeuedCounter_, batch.size());

            size_t dataWeight = WriteBatchWithExpBackoff(batch);

            StatisticsProfiler.Increment(CommittedCounter_, batch.size());
            StatisticsProfiler.Increment(CommittedDataWeightCounter_, dataWeight);
        }
    }

    size_t WriteBatchWithExpBackoff(const TStatisticsBatch& batch)
    {
        auto delay = Config_->MinRepeatDelay;
        while (true) {
            auto dropped = DroppedCount_.exchange(0);
            if (dropped) {
                LOG_WARNING("Maximum items reached, dropping job statistics (DroppedItems: %v)", dropped);
            }
            try {
                return TryWriteBatch(batch);
            } catch (const std::exception& ex) {
                LOG_WARNING(ex, "Failed to report job statistics (RetryDelay: %v)", delay.Seconds());
            }
            WaitFor(TDelayedExecutor::MakeDelayed(delay));
            delay *= 2;
            if (delay > Config_->MaxRepeatDelay) {
                delay = Config_->MaxRepeatDelay;
            }
        }
    }

    size_t TryWriteBatch(const TStatisticsBatch& batch)
    {
        LOG_DEBUG("Job statistics transaction starting "
            "(ItemCount: %v, PendingItemsNormalPriority: %v, PendingItemsLowPriority: %v)",
            batch.size(),
            NormalPriorityCounter_.InProgressCount.load(std::memory_order_relaxed),
            LowPriorityCounter_.InProgressCount.load(std::memory_order_relaxed));
        auto asyncTransaction = Client_->StartTransaction(ETransactionType::Tablet);
        auto transactionOrError = WaitFor(asyncTransaction);
        auto transaction = transactionOrError.ValueOrThrow();
        LOG_DEBUG("Job statistics transaction started (TransactionId: %v, ItemCount: %v)",
            transaction->GetId(),
            batch.size());

        std::vector<TUnversionedRow> rows;
        auto rowBuffer = New<TRowBuffer>(TJobStatisticsTag());

        size_t dataWeight = 0;
        TEnumIndexedVector<int, EReportPriority> counters = {0, 0};
        for (auto&& statistics : batch) {
            ++counters[statistics.Priority()];
            TUnversionedRowBuilder builder;
            builder.AddValue(MakeUnversionedUint64Value(statistics.OperationId().Parts64[0], Table_.Ids.OperationIdHi));
            builder.AddValue(MakeUnversionedUint64Value(statistics.OperationId().Parts64[1], Table_.Ids.OperationIdLo));
            builder.AddValue(MakeUnversionedUint64Value(statistics.JobId().Parts64[0], Table_.Ids.JobIdHi));
            builder.AddValue(MakeUnversionedUint64Value(statistics.JobId().Parts64[1], Table_.Ids.JobIdLo));
            if (statistics.Type()) {
                builder.AddValue(MakeUnversionedStringValue(*statistics.Type(), Table_.Ids.Type));
            }
            if (statistics.State()) {
                builder.AddValue(MakeUnversionedStringValue(*statistics.State(), Table_.Ids.State));
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
            if (statistics.Spec()) {
                builder.AddValue(MakeUnversionedStringValue(*statistics.Spec(), Table_.Ids.Spec));
            }
            if (statistics.SpecVersion()) {
                builder.AddValue(MakeUnversionedInt64Value(*statistics.SpecVersion(), Table_.Ids.SpecVersion));
            }
            if (statistics.Statistics()) {
                builder.AddValue(MakeUnversionedAnyValue(*statistics.Statistics(), Table_.Ids.Statistics));
            }
            if (statistics.Events()) {
                builder.AddValue(MakeUnversionedAnyValue(*statistics.Events(), Table_.Ids.Events));
            }
            rows.push_back(rowBuffer->Capture(builder.GetRow()));
            dataWeight += GetDataWeight(rows.back());
        }

        transaction->WriteRows(
            Config_->TableName,
            Table_.NameTable,
            MakeSharedRange(std::move(rows))
        );
        auto asyncResult = WaitFor(transaction->Commit());
        asyncResult.ThrowOnError();

        NormalPriorityCounter_.InProgressCount -= counters[EReportPriority::Normal];
        LowPriorityCounter_.InProgressCount -= counters[EReportPriority::Low];

        LOG_DEBUG("Job statistics transaction committed (TransactionId: %v, "
            "CommittedItemsNormalPriority: %v, CommittedItemsLowPriority: %v, CommittedDataWeight: %v)",
            transaction->GetId(),
            counters[EReportPriority::Normal],
            counters[EReportPriority::Low],
            dataWeight);
        return dataWeight;
    }
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobAgent
} // namespace NYT
