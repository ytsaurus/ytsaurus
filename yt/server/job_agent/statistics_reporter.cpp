#include "statistics_reporter.h"
#include "private.h"
#include "config.h"

#include <yt/server/data_node/master_connector.h>

#include <yt/server/cell_node/bootstrap.h>

#include <yt/ytlib/api/connection.h>
#include <yt/ytlib/api/client.h>
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

struct TStatisticsItem
{
    TOperationId OperationId;
    TJobId JobId;
    Stroka JobType;
    Stroka JobState;
    TNullable<ui64> StartTime;
    TNullable<ui64> FinishTime;
    TNullable<Stroka> Error;
    TNullable<Stroka> Statistics;
};

using TStatisticsBatch = std::vector<TStatisticsItem>;

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
            , Statistics(n->RegisterName("statistics"))
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
        const int Statistics;
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
        , Batcher_(New<TNonblockingBatch<TStatisticsItem>>(Config_->MaxItemsInBatch, Config_->ReportingPeriod))
    {
        Reporter_->GetInvoker()->Invoke(BIND(&TImpl::OnReporting, MakeWeak(this)));
    }

    void ReportStatistics(TStatisticsItem&& item)
    {
        if (!Config_->Enabled) {
            return;
        }
        if (++InProgressCount_ > Config_->MaxItemsInProgress) {
            ++DroppedCount_;
            --InProgressCount_;
            StatisticsProfiler.Increment(DroppedCounter_);
        } else {
            Batcher_->Enqueue(std::move(item));
            StatisticsProfiler.Increment(EnqueuedCounter_);
        }
    }

private:
    const TStatisticsReporterConfigPtr Config_;
    const IClientPtr Client_;
    const TActionQueuePtr Reporter_ = New<TActionQueue>("Reporter");
    const Stroka DefaultLocalAddress_;
    const TNonblockingBatchPtr<TStatisticsItem> Batcher_;
    const TStatisticsTableDescriptor Table_;

    std::atomic<int> InProgressCount_ = {0};
    std::atomic<int> DroppedCount_ = {0};

    TSimpleCounter EnqueuedCounter_ = {"/enqueued"};
    TSimpleCounter DequeuedCounter_ = {"/dequeued"};
    TSimpleCounter DroppedCounter_ = {"/dropped"};
    TSimpleCounter CommittedCounter_ = {"/committed"};
    TSimpleCounter CommittedDataWeightCounter_ = {"/committed_data_weight"};

    void OnReporting()
    {
        while (true) {
            auto asyncBatch = Batcher_->DequeueBatch();
            auto batchOrError = WaitFor(asyncBatch);
            auto batch = batchOrError.ValueOrThrow();

            StatisticsProfiler.Increment(DequeuedCounter_, batch.size());

            WriteBatchWithExpBackoff(batch);
        }
    }

    void WriteBatchWithExpBackoff(const TStatisticsBatch& batch)
    {
        auto delay = Config_->MinRepeatDelay;
        while (true) {
            auto dropped = DroppedCount_.exchange(0);
            if (dropped) {
                LOG_WARNING("Maximum items reached, dropping statistics (DroppedItems: %v)", dropped);
            }
            try {
                TryWriteBatch(batch);
                return;
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

    void TryWriteBatch(const TStatisticsBatch& batch)
    {
        LOG_DEBUG("Job statistics transaction starting (ItemCount: %v, PendingItems: %v)",
            batch.size(),
            InProgressCount_.load());
        auto asyncTransaction = Client_->StartTransaction(ETransactionType::Tablet);
        auto transactionOrError = WaitFor(asyncTransaction);
        auto transaction = transactionOrError.ValueOrThrow();
        LOG_DEBUG("Job statistics transaction started (TransactionId: %v, ItemCount: %v)",
            transaction->GetId(),
            batch.size());

        std::vector<TUnversionedRow> rows;
        auto rowBuffer = New<TRowBuffer>(TJobStatisticsTag());

        size_t dataWeight = 0;
        for (auto&& item : batch) {
            TUnversionedRowBuilder builder;
            builder.AddValue(MakeUnversionedUint64Value(item.OperationId.Parts64[0], Table_.Ids.OperationIdHi));
            builder.AddValue(MakeUnversionedUint64Value(item.OperationId.Parts64[1], Table_.Ids.OperationIdLo));
            builder.AddValue(MakeUnversionedUint64Value(item.JobId.Parts64[0], Table_.Ids.JobIdHi));
            builder.AddValue(MakeUnversionedUint64Value(item.JobId.Parts64[1], Table_.Ids.JobIdLo));
            builder.AddValue(MakeUnversionedStringValue(item.JobType, Table_.Ids.Type));
            builder.AddValue(MakeUnversionedStringValue(item.JobState, Table_.Ids.State));
            if (item.StartTime) {
                builder.AddValue(MakeUnversionedInt64Value(*item.StartTime, Table_.Ids.StartTime));
            }
            if (item.FinishTime) {
                builder.AddValue(MakeUnversionedInt64Value(*item.FinishTime, Table_.Ids.FinishTime));
            }
            builder.AddValue(MakeUnversionedStringValue(DefaultLocalAddress_, Table_.Ids.Address));
            if (item.Error) {
                builder.AddValue(MakeUnversionedAnyValue(*item.Error, Table_.Ids.Error));
            }
            if (item.Statistics) {
                builder.AddValue(MakeUnversionedAnyValue(*item.Statistics, Table_.Ids.Statistics));
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

        InProgressCount_ -= batch.size();
        StatisticsProfiler.Increment(CommittedCounter_, batch.size());
        StatisticsProfiler.Increment(CommittedDataWeightCounter_, dataWeight);
        LOG_DEBUG("Job statistics transaction committed (TransactionId: %v, CommittedItems: %v, "
            "CommittedDataWeight: %v)",
            transaction->GetId(),
            batch.size(),
            dataWeight);
    }
};

////////////////////////////////////////////////////////////////////////////////

TStatisticsReporter::TStatisticsReporter(
    TStatisticsReporterConfigPtr reporterConfig,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(
        std::move(reporterConfig),
        bootstrap))
{ }

void TStatisticsReporter::ReportStatistics(
    TOperationId operationId,
    TJobId jobId,
    EJobType jobType,
    EJobState jobState,
    const TNullable<TInstant>& startTime,
    const TNullable<TInstant>& finishTime,
    const TNullable<TError>& error,
    const TNullable<TYsonString>& statistics)
{
    Impl_->ReportStatistics({
        operationId,
        jobId,
        FormatEnum(jobType),
        FormatEnum(jobState),
        startTime ? startTime->MicroSeconds() : TNullable<ui64>(),
        finishTime ? finishTime->MicroSeconds() : TNullable<ui64>(),
        (error && !error->IsOK()) ? ConvertToYsonString(*error).Data() : TNullable<Stroka>(),
        statistics ? statistics->Data() : TNullable<Stroka>()});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobAgent
} // namespace NYT
