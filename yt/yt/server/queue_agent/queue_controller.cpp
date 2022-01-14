#include "queue_controller.h"

#include "config.h"
#include "helpers.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NQueueAgent {

using namespace NHydra;
using namespace NYTree;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

struct TQueuePartitionStatus
{
    TError Error;
    i64 TotalRowCount = -1;
    i64 TrimmedRowCount = -1;
    std::optional<TInstant> LastRowInstant;
};

void Serialize(const TQueuePartitionStatus& status, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginMap()
        .Do([&] (TFluentMap fluent) {
            if (!status.Error.IsOK()) {
                fluent
                    .Item("error").Value(status.Error);
            } else {
                fluent
                    .Item("total_row_count").Value(status.TotalRowCount)
                    .Item("trimmed_row_count").Value(status.TrimmedRowCount)
                    .OptionalItem("last_row_instant", status.LastRowInstant);
            }
        })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TQueueStatus);

struct TQueueStatus
    : public TRefCounted
{
    TQueueTableRow Row;

    TError Error;

    int PartitionCount = -1;
    std::vector<TQueuePartitionStatus> PartitionStatuses;

    static TQueueStatusPtr FromRow(TQueueTableRow row);
};

DEFINE_REFCOUNTED_TYPE(TQueueStatus);

TQueueStatusPtr TQueueStatus::FromRow(TQueueTableRow row)
{
    auto status = New<TQueueStatus>();
    status->Row = std::move(row);
    return status;
}

// NB: serializes as YSON map fragment.
void Serialize(const TQueueStatusPtr& status, IYsonConsumer* consumer)
{
    BuildYsonMapFragmentFluently(consumer)
        .Item("row").Value(status->Row)
        .Do([&] (TFluentMap fluent) {
            if (!status->Error.IsOK()) {
                fluent
                    .Item("error").Value(status->Error);
            } else {
                fluent
                    .Item("partition_count").Value(status->PartitionCount)
                    .Item("partition_statuses").Value(status->PartitionStatuses);
            }
        });
}

////////////////////////////////////////////////////////////////////////////////

struct TConsumerPartitionStatus
{
    i64 OffsetRowIndex = -1;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TConsumerStatus);

using TConsumerStatusMap = THashMap<TCrossClusterReference, TConsumerStatusPtr>;

struct TConsumerStatus
    : public TRefCounted
{
    TConsumerTableRow Row;
    TError Error;
    bool Vital = false;

    std::vector<TConsumerPartitionStatus> PartitionStatuses;

    static TConsumerStatusMap MapFromRowMap(TConsumerRowMap consumerRowMap);
};

DEFINE_REFCOUNTED_TYPE(TConsumerStatus);

TConsumerStatusMap TConsumerStatus::MapFromRowMap(TConsumerRowMap consumerRowMap)
{
    TConsumerStatusMap statusMap;
    for (auto& [ref, row] : consumerRowMap) {
        auto status = New<TConsumerStatus>();
        status->Row = std::move(row);
        status->Error = TError("Consumer is not processed yet");
        statusMap.emplace(std::move(ref), std::move(status));
    }
    return statusMap;
}

////////////////////////////////////////////////////////////////////////////////

class TOrderedDynamicTableController
    : public IQueueController
{
public:
    TOrderedDynamicTableController(
        TQueueControllerConfigPtr config,
        TClusterDirectoryPtr clusterDirectory,
        TCrossClusterReference queueRef,
        TQueueTableRow queueRow,
        TConsumerRowMap consumerRowMap,
        IInvokerPtr invoker)
        : Config_(std::move(config))
        , ClusterDirectory_(std::move(clusterDirectory))
        , QueueRef_(std::move(queueRef))
        , QueueStatus_(TQueueStatus::FromRow(std::move(queueRow)))
        , ConsumerStatuses_(TConsumerStatus::MapFromRowMap(std::move(consumerRowMap)))
        , Invoker_(std::move(invoker))
        , Logger(QueueAgentLogger.WithTag("Queue: %Qv", QueueRef_))
        , LoopExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TOrderedDynamicTableController::Loop, MakeWeak(this)),
            Config_->LoopPeriod))
    {
        QueueStatus_->Error = TError("Queue is not processed yet");
        for (const auto& consumerStatus : GetValues(ConsumerStatuses_)) {
            consumerStatus->Error = TError("Consumer is not processed yet");
        }
    }

    EQueueType GetQueueType() const override
    {
        return EQueueType::OrderedDynamicTable;
    }

    void BuildOrchid(TFluentMap fluent) const override
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (!Config_->LoopPeriod) {
            // Sync mode.
            const_cast<TOrderedDynamicTableController&>(*this).UpdateQueueStatus();
        }

        fluent
            .Item("type").Value(GetQueueType())
            .Item("loop_index").Value(LoopIndex_)
            .Item("loop_instant").Value(LoopInstant_)
            .Do([&] (TFluentMap fluent) {
                Serialize(QueueStatus_, fluent.GetConsumer());
            });
    }

    void BuildConsumerOrchid(const TCrossClusterReference& consumerRef, TFluentMap fluent) const override
    {
        Y_UNUSED(consumerRef);
        Y_UNUSED(fluent);
    }

    void Start() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_INFO("Queue controller started");

        LoopExecutor_->Start();
    }

    TFuture<void> Stop() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_INFO("Queue controller stopped");

        return LoopExecutor_->Stop();
    }

    IInvokerPtr GetInvoker() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Invoker_;
    }

private:
    const TQueueControllerConfigPtr Config_;
    const TClusterDirectoryPtr ClusterDirectory_;
    const TCrossClusterReference QueueRef_;

    TQueueStatusPtr QueueStatus_;
    TConsumerStatusMap ConsumerStatuses_;

    TQueueStatusPtr NextQueueStatus_;
    TConsumerStatusMap NextConsumerStatuses_;

    const IInvokerPtr Invoker_;

    const NLogging::TLogger Logger;

    const TPeriodicExecutorPtr LoopExecutor_;

    i64 LoopIndex_ = 0;
    TInstant LoopInstant_ = TInstant::Zero();

    void Loop()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        const auto& Logger = QueueAgentLogger;

        LoopInstant_ = TInstant::Now();
        ++LoopIndex_;

        YT_LOG_INFO("Controller loop started (LoopIndex: %v)", LoopIndex_);
        auto logFinally = Finally([&] {
            YT_LOG_INFO("Controller loop finished (LoopIndex: %v)", LoopIndex_);
        });

        std::vector<TFuture<void>> statusUpdateFutures;
        statusUpdateFutures.reserve(ConsumerStatuses_.size() + 1);
        statusUpdateFutures.emplace_back(
            BIND(&TOrderedDynamicTableController::UpdateQueueStatus, MakeWeak(this))
                .AsyncVia(Invoker_)
                .Run());
        for (const auto& consumerRef : GetKeys(ConsumerStatuses_)) {
            statusUpdateFutures.emplace_back(
                BIND(&TOrderedDynamicTableController::UpdateConsumerStatus, MakeWeak(this), consumerRef)
                    .AsyncVia(Invoker_)
                    .Run());
        }

        // None of status update methods may throw.
        YT_VERIFY(WaitFor(AllSucceeded(statusUpdateFutures)).IsOK());
    }

    void UpdateQueueStatus()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        NextQueueStatus_ = TQueueStatus::FromRow(QueueStatus_->Row);

        try {
            UpdateQueueStatusGuarded();
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            YT_LOG_DEBUG(error, "Error updating queue status");
            NextQueueStatus_->Error = std::move(error);
        }

        QueueStatus_ = std::move(NextQueueStatus_);
    }

    void UpdateQueueStatusGuarded()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto client = GetClusterClient(ClusterDirectory_, QueueRef_.Cluster);
        const auto& tableMountCache = client->GetTableMountCache();

        // Fetch partition count (which is equal to tablet count).

        const auto& tableInfo = WaitFor(tableMountCache->GetTableInfo(QueueRef_.Path))
            .ValueOrThrow();

        auto& partitionCount = NextQueueStatus_->PartitionCount;
        partitionCount = tableInfo->Tablets.size();

        auto& partitionStatuses = NextQueueStatus_->PartitionStatuses;
        partitionStatuses.resize(partitionCount);

        // Fetch tablet infos.

        std::vector<int> tabletIndexes;
        tabletIndexes.reserve(partitionCount);
        for (int index = 0; index < partitionCount; ++index) {
            const auto& tabletInfo = tableInfo->Tablets[index];
            if (tabletInfo->State != ETabletState::Mounted) {
                partitionStatuses[index].Error = TError("Tablet %v is not mounted", tabletInfo->TabletId)
                    << TErrorAttribute("state", tabletInfo->State);
            } else {
                tabletIndexes.push_back(index);
            }
        }

        // TODO(max42): timeout.
        auto tabletInfos = WaitFor(client->GetTabletInfos(QueueRef_.Path, tabletIndexes))
            .ValueOrThrow();

        YT_VERIFY(std::ssize(tabletInfos) == std::ssize(tabletIndexes));

        // Fill partition statuses from tablet infos

        for (int index = 0; index < std::ssize(tabletInfos); ++index) {
            auto& partitionStatus = partitionStatuses[tabletIndexes[index]];
            const auto& tabletInfo = tabletInfos[index];
            partitionStatus.TotalRowCount = tabletInfo.TotalRowCount;
            partitionStatus.TrimmedRowCount = tabletInfo.TrimmedRowCount;
            partitionStatus.LastRowInstant = TimestampToInstant(tabletInfo.LastWriteTimestamp).first;
        }
    }

    void UpdateConsumerStatus(TCrossClusterReference consumerRef)
    {
        Y_UNUSED(consumerRef);

        VERIFY_INVOKER_AFFINITY(Invoker_);

        // TODO(max42).
    }
};

DEFINE_REFCOUNTED_TYPE(TOrderedDynamicTableController)

////////////////////////////////////////////////////////////////////////////////

IQueueControllerPtr CreateQueueController(
    TQueueControllerConfigPtr config,
    NHiveClient::TClusterDirectoryPtr clusterDirectory,
    TCrossClusterReference queueRef,
    EQueueType queueType,
    TQueueTableRow queueRow,
    THashMap<TCrossClusterReference, TConsumerTableRow> consumerRefToRow,
    IInvokerPtr invoker)
{
    switch (queueType) {
        case EQueueType::OrderedDynamicTable:
            return New<TOrderedDynamicTableController>(
                std::move(config),
                std::move(clusterDirectory),
                std::move(queueRef),
                std::move(queueRow),
                std::move(consumerRefToRow),
                std::move(invoker));
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
