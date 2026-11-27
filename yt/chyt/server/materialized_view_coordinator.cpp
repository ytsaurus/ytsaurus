#include "materialized_view_coordinator.h"

#include "config.h"
#include "cypress_object_repository.h"
#include "helpers.h"
#include "host.h"
#include "storage_yt_materialized_view.h"

#include <yt/chyt/client/query_service_proxy.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/cypress_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/queue_client/consumer_client.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/actions/current_invoker.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/rpc/helpers.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <util/random/random.h>

#include <algorithm>
#include <numeric>

namespace NYT::NClickHouseServer {

using namespace NApi;
using namespace NApi::NNative;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ClickHouseYtLogger;

namespace {

void MountConsumerAndWait(
    const NNative::IClientPtr& client,
    const TYPath& consumerPath,
    TDuration timeout)
{
    auto deadline = TInstant::Now() + timeout;
    auto getTabletState = [&] {
        return ConvertTo<NTabletClient::ETabletState>(
            WaitFor(client->GetNode(consumerPath + "/@tablet_state")).ValueOrThrow());
    };

    if (getTabletState() == NTabletClient::ETabletState::Mounted) {
        return;
    }

    WaitFor(client->MountTable(consumerPath)).ThrowOnError();

    while (getTabletState() != NTabletClient::ETabletState::Mounted) {
        THROW_ERROR_EXCEPTION_IF(TInstant::Now() >= deadline,
            "Timed out waiting for materialized view queue consumer to become mounted")
            .With("consumer_path", consumerPath)
            .With("timeout", timeout);

        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TMaterializedViewPartitionProgress
    : public TYsonStruct
{
    TObjectId ObjectId;
    std::optional<int> PartitionIndex;
    i64 NextRowIndex = 0;
    i64 TotalRowCount = 0;
    std::optional<TInstant> LastUpdate;
    std::string LastError;

    REGISTER_YSON_STRUCT(TMaterializedViewPartitionProgress);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("object_id", &TThis::ObjectId);
        registrar.Parameter("partition_index", &TThis::PartitionIndex)
            .Default();
        registrar.Parameter("next_row_index", &TThis::NextRowIndex)
            .Default(0);
        registrar.Parameter("total_row_count", &TThis::TotalRowCount)
            .Default(0);
        registrar.Parameter("last_update", &TThis::LastUpdate)
            .Default();
        registrar.Parameter("last_error", &TThis::LastError)
            .Default();
    }
};

DECLARE_REFCOUNTED_STRUCT(TMaterializedViewPartitionProgress);
DEFINE_REFCOUNTED_TYPE(TMaterializedViewPartitionProgress)

struct TMaterializedViewProgress
    : public TYsonStruct
{
    std::vector<TMaterializedViewPartitionProgressPtr> Partitions;
    std::string LastError;
    bool QueueConsumerInitialized = false;
    int NextPartitionIndex = 0;

    REGISTER_YSON_STRUCT(TMaterializedViewProgress);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("partitions", &TThis::Partitions);
        registrar.Parameter("last_error", &TThis::LastError)
            .Default();
        registrar.Parameter("queue_consumer_initialized", &TThis::QueueConsumerInitialized)
            .Default(false);
        registrar.Parameter("next_partition_index", &TThis::NextPartitionIndex)
            .GreaterThanOrEqual(0)
            .Default(0);
    }
};

DECLARE_REFCOUNTED_STRUCT(TMaterializedViewProgress)
DEFINE_REFCOUNTED_TYPE(TMaterializedViewProgress)

class TMaterializedViewProgressStore
    : public TRefCounted
{
public:
    explicit TMaterializedViewProgressStore(TYPath rootPath)
        : ProgressRootPath_(rootPath + "/progress")
        , ConsumerRootPath_(rootPath + "/consumers")
        , ReadCounter_(ClickHouseYtProfiler().Counter("/materialized_views/progress_read_count"))
        , WriteCounter_(ClickHouseYtProfiler().Counter("/materialized_views/progress_write_count"))
    { }

    void EnsureReady(const IClientBasePtr& client)
    {
        TCreateNodeOptions options;
        options.IgnoreExisting = true;
        options.Recursive = true;
        WaitFor(client->CreateNode(ProgressRootPath_, EObjectType::MapNode, options))
            .ThrowOnError();
        WaitFor(client->CreateNode(ConsumerRootPath_, EObjectType::MapNode, options))
            .ThrowOnError();
    }

    TErrorOr<TMaterializedViewProgressPtr> GetProgress(
        const IClientBasePtr& client,
        TObjectId viewId) const
    {
        ReadCounter_.Increment();
        TGetNodeOptions options;
        options.ReadFrom = EMasterChannelKind::Leader;
        return WaitFor(client->GetNode(GetProgressNodePath(viewId), options)
            .Apply(BIND([] (const NYson::TYsonString& yson) {
                return ConvertTo<TMaterializedViewProgressPtr>(yson);
            })));
    }

    void CreateProgress(
        const NApi::ITransactionPtr& transaction,
        TObjectId viewId,
        TMaterializedViewProgressPtr progress) const
    {
        TCreateNodeOptions options;
        options.Attributes = CreateEphemeralAttributes();
        options.Attributes->Set("value", std::move(progress));
        WaitFor(transaction->CreateNode(
            GetProgressNodePath(viewId),
            EObjectType::Document,
            options))
            .ThrowOnError();
    }

    void SetProgress(
        const NApi::ITransactionPtr& transaction,
        TObjectId viewId,
        TMaterializedViewProgressPtr progress) const
    {
        WriteCounter_.Increment();
        WaitFor(transaction->SetNode(
            GetProgressNodePath(viewId),
            NYson::ConvertToYsonString(progress)))
            .ThrowOnError();
    }

    TError SetError(
        const IClientBasePtr& client,
        TObjectId viewId,
        const TError& error,
        TTransactionId leadershipTransactionId) const
    {
        WriteCounter_.Increment();
        TSetNodeOptions options;
        if (leadershipTransactionId) {
            options.PrerequisiteTransactionIds.push_back(leadershipTransactionId);
        }
        return WaitFor(client->SetNode(
            GetProgressNodePath(viewId) + "/last_error",
            NYson::ConvertToYsonString(error.GetMessage()),
            options));
    }

    void CreateQueueConsumer(
        const IClientBasePtr& client,
        TObjectId viewId) const
    {
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("dynamic", true);
        attributes->Set("schema", NQueueClient::GetConsumerSchema());
        attributes->Set("treat_as_queue_consumer", true);

        TCreateNodeOptions options;
        options.Attributes = std::move(attributes);
        WaitFor(client->CreateNode(GetConsumerPath(viewId), EObjectType::Table, options))
            .ThrowOnError();
    }

    TYPath PrepareQueueConsumer(
        const NNative::IClientPtr& client,
        TObjectId viewId,
        const TYPath& queuePath,
        bool queueConsumerInitialized,
        TDuration mountTimeout) const
    {
        auto consumerPath = GetConsumerPath(viewId);
        MountConsumerAndWait(client, consumerPath, mountTimeout);

        if (!queueConsumerInitialized) {
            WaitFor(client->RegisterQueueConsumer(queuePath, consumerPath, /*vital*/ true))
                .ThrowOnError();
        }

        return consumerPath;
    }

    TYPath GetProgressNodePath(TObjectId viewId) const
    {
        return ProgressRootPath_ + "/" + ToYPathLiteral(ToString(viewId));
    }

private:
    const TYPath ProgressRootPath_;
    const TYPath ConsumerRootPath_;
    const NProfiling::TCounter ReadCounter_;
    const NProfiling::TCounter WriteCounter_;

    TYPath GetConsumerPath(TObjectId viewId) const
    {
        return ConsumerRootPath_ + "/" + ToYPathLiteral(ToString(viewId));
    }
};

DECLARE_REFCOUNTED_CLASS(TMaterializedViewProgressStore)
DEFINE_REFCOUNTED_TYPE(TMaterializedViewProgressStore)

struct TPartitionInfo
{
    TObjectId ObjectId;
    std::optional<int> PartitionIndex;
    i64 RowCount = 0;
};


std::vector<TPartitionInfo> FetchPartitionInfos(
    const NNative::IClientPtr& client,
    TTransactionId transactionId,
    EMaterializedViewSourceType sourceType,
    const TYPath& sourcePath,
    const TMasterReadOptions& masterReadOptions)
{
    if (sourceType == EMaterializedViewSourceType::StaticTable ||
        sourceType == EMaterializedViewSourceType::Queue)
    {
        TGetNodeOptions options;
        static_cast<TMasterReadOptions&>(options) = masterReadOptions;
        options.TransactionId = transactionId;
        options.Attributes = {"id", "type", "dynamic", "row_count", "tablet_count"};
        auto node = ConvertToNode(WaitFor(client->GetNode(sourcePath + "/@", options)).ValueOrThrow())->AsMap();
        if (node->GetChildValueOrThrow<EObjectType>("type") != EObjectType::Table) {
            THROW_ERROR_EXCEPTION("Source is not a table")
                .With("source_path", sourcePath);
        }

        auto dynamic = node->GetChildValueOrDefault<bool>("dynamic", false);
        auto expectedDynamic = sourceType == EMaterializedViewSourceType::Queue;
        THROW_ERROR_EXCEPTION_IF(dynamic != expectedDynamic,
            "Materialized view source kind changed")
            .With("source_path", sourcePath)
            .With("expected_dynamic", expectedDynamic)
            .With("actual_dynamic", dynamic);

        auto objectId = node->GetChildValueOrThrow<TObjectId>("id");
        if (sourceType == EMaterializedViewSourceType::StaticTable) {
            auto rowCount = node->FindChildValue<i64>("row_count");
            THROW_ERROR_EXCEPTION_IF(!rowCount,
                "Materialized view source partition has no row count")
                .With("source_path", sourcePath);
            return {TPartitionInfo{
                .ObjectId = objectId,
                .RowCount = *rowCount,
            }};
        }

        auto tabletCount = node->GetChildValueOrThrow<int>("tablet_count");
        std::vector<int> tabletIndexes(tabletCount);
        std::iota(tabletIndexes.begin(), tabletIndexes.end(), 0);
        auto tabletInfos = WaitFor(client->GetTabletInfos(sourcePath, tabletIndexes))
            .ValueOrThrow();

        std::vector<TPartitionInfo> partitions;
        partitions.reserve(tabletInfos.size());
        for (int index = 0; index < std::ssize(tabletInfos); ++index) {
            partitions.push_back({
                .ObjectId = objectId,
                .PartitionIndex = index,
                .RowCount = tabletInfos[index].TotalRowCount,
            });
        }
        return partitions;
    }

    if (sourceType == EMaterializedViewSourceType::TableRange) {
        TListNodeOptions options;
        static_cast<TMasterReadOptions&>(options) = masterReadOptions;
        options.TransactionId = transactionId;
        options.Attributes = {"id", "type", "dynamic", "row_count"};
        auto children = ConvertTo<IListNodePtr>(WaitFor(client->ListNode(sourcePath, options))
            .ValueOrThrow())
            ->GetChildren();

        std::vector<TPartitionInfo> partitions;
        for (const auto& child : children) {
            const auto& attributes = child->Attributes();
            if (attributes.Get<EObjectType>("type") != EObjectType::Table) {
                continue;
            }
            auto objectId = attributes.Get<TObjectId>("id");
            THROW_ERROR_EXCEPTION_IF(attributes.Find<bool>("dynamic").value_or(false),
                "Materialized view source partition %v must be static", objectId)
                .With("source_path", sourcePath)
                .With("child_object_id", objectId);
            auto rowCount = attributes.Find<i64>("row_count");
            THROW_ERROR_EXCEPTION_IF(!rowCount,
                "Materialized view source partition has no row count")
                .With("source_path", sourcePath)
                .With("source_type", sourceType)
                .With("object_id", objectId);
            partitions.push_back({
                .ObjectId = objectId,
                .RowCount = *rowCount,
            });
        }
        return partitions;
    }

    YT_ABORT();
}

struct TRefreshTask
{
    TObjectId ObjectId;
    int PartitionIndex = -1;
    i64 LowerRowIndex = 0;
    i64 UpperRowIndex = 0;
};

struct TRefreshResult
{
    int PartitionIndex = -1;
    i64 OldOffset = 0;
    TErrorOr<i64> Result;
};

TMaterializedViewProgressPtr TryPrepareNextProgress(
    const TMaterializedViewProgressPtr& currentProgress,
    const std::vector<TPartitionInfo>& partitionInfos,
    bool initializeQueueConsumer)
{
    THashMap<std::pair<TObjectId, std::optional<int>>, TMaterializedViewPartitionProgressPtr> partitionProgresses;
    bool hasPendingRows = false;
    for (const auto& partition : currentProgress->Partitions) {
        hasPendingRows |= partition->NextRowIndex < partition->TotalRowCount;
        partitionProgresses.emplace(
            std::pair(partition->ObjectId, partition->PartitionIndex),
            partition);
    }

    bool sourceChanged = initializeQueueConsumer ||
        currentProgress->Partitions.size() != partitionInfos.size() ||
        std::ranges::any_of(partitionInfos, [&] (const auto& info) {
            auto it = partitionProgresses.find(std::pair(info.ObjectId, info.PartitionIndex));
            return it == partitionProgresses.end() || it->second->TotalRowCount != info.RowCount;
        });
    if (!hasPendingRows && !sourceChanged && currentProgress->LastError.empty()) {
        return nullptr;
    }
    if (!sourceChanged) {
        return CloneYsonStruct(currentProgress);
    }

    std::vector<TMaterializedViewPartitionProgressPtr> partitions;
    partitions.reserve(partitionInfos.size());
    for (const auto& info : partitionInfos) {
        TMaterializedViewPartitionProgressPtr partition;
        auto key = std::pair(info.ObjectId, info.PartitionIndex);
        auto it = partitionProgresses.find(key);
        if (it != partitionProgresses.end()) {
            partition = CloneYsonStruct(it->second);
        } else {
            partition = New<TMaterializedViewPartitionProgress>();
            partition->ObjectId = info.ObjectId;
            partition->PartitionIndex = info.PartitionIndex;
        }

        THROW_ERROR_EXCEPTION_IF(info.RowCount < partition->TotalRowCount,
            "Partition is not append-only")
            .With("object_id", info.ObjectId)
            .With("partition_index", info.PartitionIndex);

        partition->TotalRowCount = info.RowCount;
        partitions.push_back(std::move(partition));
    }

    auto progress = New<TMaterializedViewProgress>();
    progress->LastError = currentProgress->LastError;
    progress->QueueConsumerInitialized = true;
    progress->NextPartitionIndex = currentProgress->NextPartitionIndex;
    progress->Partitions = std::move(partitions);

    return progress;
}

std::vector<TRefreshTask> BuildTasks(
    const TMaterializedViewProgressPtr& nextProgress,
    int maxPartitions,
    i64 maxRows)
{
    std::vector<TRefreshTask> tasks;
    int partitionCount = std::ssize(nextProgress->Partitions);
    int maxTaskCount = maxPartitions > 0
        ? std::min(partitionCount, maxPartitions)
        : partitionCount;
    tasks.reserve(maxTaskCount);
    int startIndex = nextProgress->NextPartitionIndex;
    for (int offset = 0; offset < partitionCount && std::ssize(tasks) < maxTaskCount; ++offset) {
        int index = (startIndex + offset) % partitionCount;
        const auto& progress = nextProgress->Partitions[index];
        if (progress->NextRowIndex == progress->TotalRowCount) {
            continue;
        }

        auto lowerRowIndex = progress->NextRowIndex;
        auto upperRowIndex = progress->TotalRowCount;
        if (maxRows > 0) {
            upperRowIndex = lowerRowIndex + std::min(maxRows, upperRowIndex - lowerRowIndex);
        }

        tasks.push_back({
            .ObjectId = progress->ObjectId,
            .PartitionIndex = progress->PartitionIndex.value_or(index),
            .LowerRowIndex = lowerRowIndex,
            .UpperRowIndex = upperRowIndex,
        });
    }

    return tasks;
}

TMaterializedViewProgressPtr ApplyRefreshResults(
    const TMaterializedViewProgressPtr& currentProgress,
    TMaterializedViewProgressPtr progress,
    const std::vector<TRefreshResult>& results,
    TInstant now)
{
    for (const auto& result : results) {
        auto& partition = progress->Partitions[result.PartitionIndex];
        if (!result.Result.IsOK()) {
            partition->LastError = result.Result.GetMessage();
            continue;
        }

        partition->LastUpdate = now;
        partition->LastError.clear();
        partition->NextRowIndex = result.Result.Value();
    }

    if (!results.empty()) {
        progress->NextPartitionIndex = (results.back().PartitionIndex + 1) % std::ssize(progress->Partitions);
    }

    bool allPartitionsFailed = !progress->Partitions.empty();
    for (const auto& partition : progress->Partitions) {
        if (partition->LastError.empty()) {
            allPartitionsFailed = false;
            break;
        }
    }

    progress->LastError = allPartitionsFailed ? "All materialized view partitions are in a failed state" : "";

    if (results.empty() && *progress == *currentProgress) {
        return nullptr;
    }
    return progress;
}

class TMaterializedViewRefreshHandler
{
public:
    TMaterializedViewRefreshHandler(
        THost* host,
        NRpc::IChannelFactoryPtr channelFactory,
        TMaterializedViewsConfigPtr config,
        TMaterializedViewProgressStorePtr progressStore,
        TMasterReadOptions masterReadOptions,
        TTransactionId leadershipTransactionId,
        NProfiling::TCounter refreshCounter)
        : Host_(host)
        , ChannelFactory_(std::move(channelFactory))
        , Config_(std::move(config))
        , ProgressStore_(std::move(progressStore))
        , MasterReadOptions_(std::move(masterReadOptions))
        , LeadershipTransactionId_(leadershipTransactionId)
        , RefreshCounter_(std::move(refreshCounter))
    { }

    void Execute(const TCypressObjectRepository::TMaterializedView& view);

private:
    class TRefreshAttempt;

    THost* const Host_;
    const NRpc::IChannelFactoryPtr ChannelFactory_;
    const TMaterializedViewsConfigPtr Config_;
    const TMaterializedViewProgressStorePtr ProgressStore_;
    const TMasterReadOptions MasterReadOptions_;
    const TTransactionId LeadershipTransactionId_;
    const NProfiling::TCounter RefreshCounter_;

    TMaterializedViewProgressPtr CachedProgress_;
};

class TMaterializedViewRefreshHandler::TRefreshAttempt
{
public:
    TRefreshAttempt(
        TMaterializedViewRefreshHandler* handler,
        const TCypressObjectRepository::TMaterializedView& view)
        : Handler_(handler)
        , View_(view)
        , CurrentProgress_(handler->CachedProgress_)
    { }

    void Execute()
    {
        try {
            DoExecute();
        } catch (const std::exception& ex) {
            OnRefreshError(TError(ex));
            return;
        }

        auto error = FinishRefresh();
        if (error.IsOK() && !RefreshResults_.empty()) {
            error = FinishRefreshTasks();
        }
        if (!error.IsOK()) {
            Handler_->CachedProgress_.Reset();
            YT_TLOG_WARNING("Failed to finish materialized view refresh")
                .With("View", View_.ObjectName)
                .With(error);
        }
    }

private:
    TMaterializedViewRefreshHandler* const Handler_;
    const TCypressObjectRepository::TMaterializedView& View_;
    const TMaterializedViewProgressPtr CurrentProgress_;

    NNative::IClientPtr Client_;
    NApi::ITransactionPtr Transaction_;
    TMaterializedViewProgressPtr NextProgress_;
    NQueueClient::ISubConsumerClientPtr SubConsumerClient_;
    std::vector<TRefreshResult> RefreshResults_;

    void OnRefreshError(const TError& error)
    {
        YT_TLOG_WARNING("Materialized view refresh failed")
            .With("View", View_.ObjectName)
            .With(error);

        if (Transaction_) {
            auto abortError = WaitFor(Transaction_->Abort());
            if (!abortError.IsOK()) {
                YT_TLOG_WARNING("Failed to abort materialized view refresh transaction")
                    .With("View", View_.ObjectName)
                    .With(abortError);
            }
            Transaction_.Reset();
        }

        auto setError = Handler_->ProgressStore_->SetError(
            Handler_->Host_->GetRootClient(),
            View_.ObjectId,
            error,
            Handler_->LeadershipTransactionId_);
        if (!setError.IsOK()) {
            YT_TLOG_WARNING("Failed to persist materialized view refresh error")
                .With("View", View_.ObjectName)
                .With(setError);
            Handler_->CachedProgress_.Reset();
        } else {
            Handler_->CachedProgress_->LastError = error.GetMessage();
        }
    }

    TError FinishRefresh()
    {
        if (!NextProgress_) {
            return WaitFor(Transaction_->Abort());
        }

        auto error = WaitFor(Transaction_->Commit());
        if (!error.IsOK()) {
            auto abortError = WaitFor(Transaction_->Abort());
            if (!abortError.IsOK()) {
                YT_TLOG_WARNING("Failed to abort materialized view refresh transaction")
                    .With("View", View_.ObjectName)
                    .With(abortError);
            }
            return error;
        }
        if (Handler_->Host_->GetConfig()->QuerySettings->Testing->ThrowExceptionAfterRefreshCommit) {
            return TError("Testing exception after materialized view refresh transaction commit");
        }

        Handler_->CachedProgress_ = std::move(NextProgress_);
        return {};
    }

    TError FinishRefreshTasks()
    {
        const auto& testingConfig = Handler_->Host_->GetConfig()->QuerySettings->Testing;
        try {
            if (auto breakpointFilename = testingConfig->MaterializedViewConsumerCommitBreakpoint) {
                HandleBreakpoint(*breakpointFilename, Handler_->Host_->GetRootClient());
            }
        } catch (const std::exception& ex) {
            return TError(ex);
        }

        auto error = CommitConsumerPersistedOffsets();
        if (!error.IsOK()) {
            return error;
        }

        auto successCount = std::count_if(RefreshResults_.begin(), RefreshResults_.end(), [] (const auto& result) {
            return result.Result.IsOK();
        });
        YT_TLOG_INFO("Materialized view refresh completed")
            .With("View", View_.ObjectName)
            .With("SuccessfulPartitionCount", successCount)
            .With("FailedPartitionCount", std::ssize(RefreshResults_) - successCount);
        return {};
    }

    bool StartRefresh()
    {
        Client_ = Handler_->Host_->CreateClient(View_.Creator);

        TTransactionStartOptions options;
        options.Timeout = Handler_->Config_->TransactionTimeout;
        if (Handler_->LeadershipTransactionId_) {
            options.PrerequisiteTransactionIds.push_back(Handler_->LeadershipTransactionId_);
        }
        Transaction_ = WaitFor(Client_->StartTransaction(ETransactionType::Master, options))
            .ValueOrThrow();

        auto refreshLockPath = Handler_->ProgressStore_->GetProgressNodePath(View_.ObjectId);
        auto refreshLockOrError = WaitFor(Transaction_->LockNode(refreshLockPath, ELockMode::Exclusive));
        if (refreshLockOrError.FindMatching(NCypressClient::EErrorCode::ConcurrentTransactionLockConflict)) {
            return false;
        }
        refreshLockOrError.ThrowOnError();

        return true;
    }

    void DoExecute()
    {
        if (!StartRefresh()) {
            return;
        }

        auto sourceObjectId = WaitFor(Transaction_->LockNode(View_.SourcePath, ELockMode::Snapshot))
            .ValueOrThrow().NodeId;

        std::optional<TYPath> consumerPath;
        if (View_.SourceType == EMaterializedViewSourceType::Queue) {
            consumerPath = Handler_->ProgressStore_->PrepareQueueConsumer(
                Client_,
                View_.ObjectId,
                View_.SourcePath,
                CurrentProgress_->QueueConsumerInitialized,
                Handler_->Config_->TableMountTimeout);
        }

        auto partitionInfos = FetchPartitionInfos(
            Client_,
            Transaction_->GetId(),
            View_.SourceType,
            FromObjectId(sourceObjectId),
            Handler_->MasterReadOptions_);

        auto nextProgress = TryPrepareNextProgress(
            CurrentProgress_,
            partitionInfos,
            consumerPath.has_value() && !CurrentProgress_->QueueConsumerInitialized);
        if (consumerPath) {
            SubConsumerClient_ = NQueueClient::CreateSubConsumerClient(
                Client_,
                Client_,
                std::move(*consumerPath),
                View_.SourcePath);
            RecoverConsumerOffsetsIfNeeded(nextProgress ? nextProgress : CurrentProgress_);
        }
        if (!nextProgress) {
            return;
        }

        auto tasks = BuildTasks(
            nextProgress,
            Handler_->Config_->MaxPartitionsPerRefresh,
            Handler_->Config_->MaxRowsPerRefresh);

        if (View_.SourceType == EMaterializedViewSourceType::TableRange) {
            std::vector<TFuture<TLockNodeResult>> lockFutures;
            lockFutures.reserve(tasks.size());
            for (const auto& task : tasks) {
                lockFutures.push_back(
                    Transaction_->LockNode(FromObjectId(task.ObjectId), ELockMode::Snapshot));
            }
            WaitFor(AllSucceeded(std::move(lockFutures)))
                .ThrowOnError();
        }

        RefreshResults_ = RunRefreshTasks(tasks);

        for (const auto& result : RefreshResults_) {
            if (!result.Result.IsOK()) {
                YT_TLOG_WARNING("Materialized view partition refresh failed")
                    .With("View", View_.ObjectName)
                    .With("PartitionIndex", result.PartitionIndex)
                    .With(result.Result);
            }
        }
        NextProgress_ = ApplyRefreshResults(
            CurrentProgress_,
            std::move(nextProgress),
            RefreshResults_,
            TInstant::Now());

        if (NextProgress_) {
            Handler_->ProgressStore_->SetProgress(
                Transaction_,
                View_.ObjectId,
                NextProgress_);
        }
    }

    void ValidateTarget(TObjectId targetId) const
    {
        TGetNodeOptions options;
        static_cast<TMasterReadOptions&>(options) = Handler_->MasterReadOptions_;
        options.Attributes = {"type", "dynamic"};
        auto node = ConvertToNode(WaitFor(Transaction_->GetNode(FromObjectId(targetId) + "/@", options))
            .ValueOrThrow())->AsMap();

        if (node->GetChildValueOrThrow<EObjectType>("type") != EObjectType::Table) {
            THROW_ERROR_EXCEPTION("Materialized view target %v must be a table", View_.TargetPath)
                .With("target_path", View_.TargetPath);
        }

        if (node->GetChildValueOrDefault<bool>("dynamic", false)) {
            THROW_ERROR_EXCEPTION("Materialized view target table must be static")
                .With("target_path", View_.TargetPath);
        }
    }

    DB::ASTPtr BuildTaskSource(const TRefreshTask& task, const std::string& alias) const
    {
        TReadLimit lower;
        lower.SetRowIndex(task.LowerRowIndex);
        TReadLimit upper;
        upper.SetRowIndex(task.UpperRowIndex);
        if (View_.SourceType == EMaterializedViewSourceType::Queue) {
            lower.SetTabletIndex(task.PartitionIndex);
            upper.SetTabletIndex(task.PartitionIndex);
        }
        TRichYPath rangedSourcePath(FromObjectId(task.ObjectId));
        rangedSourcePath.SetRanges({TReadRange(std::move(lower), std::move(upper))});

        auto table = std::make_shared<DB::ASTTableIdentifier>(ToString(rangedSourcePath));
        if (!alias.empty()) {
            table->setAlias(alias);
        }

        return table;
    }

    std::vector<std::string> BuildRefreshQueries(
        TObjectId targetObjectId,
        const std::vector<TRefreshTask>& tasks) const
    {
        auto insertQuery = std::make_shared<DB::ASTInsertQuery>();
        insertQuery->table_id.table_name = FromObjectId(targetObjectId);
        insertQuery->select = View_.CreateQuery->as<const DB::ASTCreateQuery&>().select->clone();
        insertQuery->children.push_back(insertQuery->select);

        auto& selectWithUnion = insertQuery->select->as<DB::ASTSelectWithUnionQuery&>();
        if (!selectWithUnion.list_of_selects || selectWithUnion.list_of_selects->children.size() != 1) {
            THROW_ERROR_EXCEPTION("Materialized view SELECT must contain a single query");
        }
        auto& select = selectWithUnion.list_of_selects->children[0]->as<DB::ASTSelectQuery&>();
        auto* tableExpression = GetSingleTableExpressionOrThrow(&select);

        auto alias = tableExpression->table_function
            ? tableExpression->table_function->tryGetAlias()
            : tableExpression->database_and_table_name->tryGetAlias();
        tableExpression->table_function.reset();

        std::vector<std::string> queries;
        queries.reserve(tasks.size());
        for (const auto& task : tasks) {
            tableExpression->database_and_table_name = BuildTaskSource(task, alias);
            queries.push_back(insertQuery->formatWithSecretsOneLine());
        }
        return queries;
    }

    std::vector<TRefreshResult> RunRefreshTasks(const std::vector<TRefreshTask>& tasks)
    {
        if (tasks.empty()) {
            return {};
        }

        auto targetLock = WaitFor(Transaction_->LockNode(View_.TargetPath, ELockMode::Shared));
        if (targetLock.FindMatching(NCypressClient::EErrorCode::ConcurrentTransactionLockConflict)) {
            return {};
        }
        auto targetObjectId = targetLock.ValueOrThrow().NodeId;
        ValidateTarget(targetObjectId);

        auto queries = BuildRefreshQueries(targetObjectId, tasks);
        std::vector<TFuture<TRefreshResult>> taskFutures;
        taskFutures.reserve(tasks.size());
        for (int index = 0; index < std::ssize(tasks); ++index) {
            taskFutures.push_back(BIND([this, task = tasks[index], query = std::move(queries[index])] () mutable {
                TTransactionStartOptions options;
                options.ParentId = Transaction_->GetId();
                options.Timeout = Handler_->Config_->TransactionTimeout;
                auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Master, options))
                    .ValueOrThrow();

                TRefreshResult result{
                    .PartitionIndex = task.PartitionIndex,
                    .OldOffset = task.LowerRowIndex,
                    .Result = task.UpperRowIndex,
                };

                auto error = WaitFor(ExecuteRefreshQuery(std::move(query), task.PartitionIndex, transaction->GetId()));
                if (!error.IsOK()) {
                    // Parent commit aborts any nested transaction whose explicit abort did not succeed.
                    YT_UNUSED_FUTURE(transaction->Abort());
                    result.Result = std::move(error);
                } else {
                    WaitFor(transaction->Commit())
                        .ThrowOnError();
                }
                return result;
            }).AsyncVia(GetCurrentInvoker()).Run());
        }

        auto taskResults = WaitFor(AllSet(std::move(taskFutures)))
            .ValueOrThrow();
        std::vector<TRefreshResult> results;
        results.reserve(taskResults.size());
        for (const auto& result : taskResults) {
            results.push_back(result.ValueOrThrow());
        }

        THROW_ERROR_EXCEPTION_IF(Handler_->Host_->GetConfig()->QuerySettings->Testing->ThrowExceptionAfterRefreshQuery,
            "Testing exception after materialized view refresh query");

        return results;
    }

    TQueryServiceProxy::TReqExecuteQueryPtr CreateRefreshQueryRequest(
        const std::string& query,
        TTransactionId transactionId)
    {
        auto instances = Handler_->Host_->GetDiscoveryNodes();
        if (instances.empty()) {
            THROW_ERROR_EXCEPTION("Cannot execute query since there are no active clique instances");
        }

        auto instanceIt = instances.begin();
        std::advance(instanceIt, RandomNumber<size_t>(instances.size()));
        const auto& [instanceId, attributes] = *instanceIt;
        auto instanceCookie = attributes->Get<int>("job_cookie");
        auto endpoint = NNet::BuildServiceAddress(
            attributes->Get<TString>("host"),
            attributes->Get<int>("rpc_port"));

        YT_TLOG_INFO("Executing materialized view refresh query on clique instance")
            .With("View", View_.ObjectName)
            .With("InstanceId", instanceId)
            .With("InstanceCookie", instanceCookie)
            .With("Endpoint", endpoint);

        TQueryServiceProxy proxy(Handler_->ChannelFactory_->CreateChannel(endpoint));
        proxy.SetDefaultTimeout(Handler_->Config_->QueryTimeout);

        auto req = proxy.ExecuteQuery();
        NRpc::SetAuthenticationIdentity(req, NRpc::TAuthenticationIdentity(View_.Creator));
        ToProto(req->mutable_query_id(), TQueryId::Create());
        ToProto(req->mutable_parent_transaction_id(), transactionId);
        auto* chytRequest = req->mutable_chyt_request();
        chytRequest->set_query(query);
        if (View_.SourceType == EMaterializedViewSourceType::Queue) {
            (*chytRequest->mutable_settings())["chyt.dynamic_table.enable_dynamic_store_read"] = "1";
        }

        return req;
    }

    TFuture<void> ExecuteRefreshQuery(
        std::string query,
        int partitionIndex,
        TTransactionId transactionId)
    {
        const auto& testingConfig = Handler_->Host_->GetConfig()->QuerySettings->Testing;
        bool failResponse = testingConfig->FailMaterializedViewRefreshQueryResponseForPartition == partitionIndex;
        return OKFuture
            .Apply(BIND([this, query = std::move(query), transactionId] {
                return CreateRefreshQueryRequest(query, transactionId)->Invoke();
            }))
            .Apply(BIND([failResponse] (const TQueryServiceProxy::TRspExecuteQueryPtr& response) {
                FromProto<TError>(response->error()).ThrowOnError();
                THROW_ERROR_EXCEPTION_IF(failResponse, "Testing lost materialized view refresh query response");
            }));
    }

    void RecoverConsumerOffsetsIfNeeded(const TMaterializedViewProgressPtr& progress)
    {
        auto partitionCount = std::ssize(progress->Partitions);
        auto partitionInfos = WaitFor(SubConsumerClient_->CollectPartitions(partitionCount))
            .ValueOrThrow();
        NApi::ITransactionPtr transaction;
        for (const auto& info : partitionInfos) {
            auto nextRowIndex = progress->Partitions[info.PartitionIndex]->NextRowIndex;
            if (info.NextRowIndex >= nextRowIndex) {
                continue;
            }

            if (!transaction) {
                transaction = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
                    .ValueOrThrow();
            }
            SubConsumerClient_->Advance(transaction, info.PartitionIndex, info.NextRowIndex, nextRowIndex);
        }
        if (transaction) {
            WaitFor(transaction->Commit())
                .ThrowOnError();
        }
    }

    TError CommitConsumerPersistedOffsets()
    {
        try {
            if (!SubConsumerClient_) {
                return {};
            }

            auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
                .ValueOrThrow();
            for (const auto& result : RefreshResults_) {
                if (!result.Result.IsOK()) {
                    continue;
                }
                SubConsumerClient_->Advance(transaction, result.PartitionIndex, result.OldOffset, result.Result.Value());
            }
            WaitFor(transaction->Commit())
                .ThrowOnError();
            return {};
        } catch (const std::exception& ex) {
            return TError(ex);
        }
    }
};

void TMaterializedViewRefreshHandler::Execute(const TCypressObjectRepository::TMaterializedView& view)
{
    if (!CachedProgress_) {
        auto progressOrError = ProgressStore_->GetProgress(Host_->GetRootClient(), view.ObjectId);
        if (!progressOrError.IsOK()) {
            YT_TLOG_WARNING("Failed to read materialized view progress")
                .With("View", view.ObjectName)
                .With(progressOrError);
            return;
        }
        CachedProgress_ = std::move(progressOrError.Value());
    }

    TRefreshAttempt(this, view).Execute();
    RefreshCounter_.Increment();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TMaterializedViewCoordinator::TImpl
    : public TRefCounted
{
public:
    TImpl(
        THost* host,
        TCypressObjectRepositoryPtr repository,
        TMaterializedViewsConfigPtr config,
        NRpc::IChannelFactoryPtr channelFactory)
        : Host_(host)
        , Repository_(std::move(repository))
        , Config_(std::move(config))
        , ChannelFactory_(std::move(channelFactory))
        , ProgressStore_(New<TMaterializedViewProgressStore>(Config_->RootPath))
        , MasterReadOptions_(*Host_->GetConfig()->TableAttributeCache->MasterReadOptions)
        , ActionQueue_(New<TActionQueue>("MaterializedViews"))
        , Invoker_(ActionQueue_->GetInvoker())
        , PeriodicExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TImpl::ScanNonThrowing, MakeWeak(this)),
            Config_->ScanPeriod))
        , RefreshCounter_(ClickHouseYtProfiler().Counter("/materialized_views/refresh_count"))
    { }

    void Start()
    {
        ProgressStore_->EnsureReady(Host_->GetRootClient());
        PeriodicExecutor_->Start();
    }

    void InitializeProgress(
        const NNative::IClientPtr& client,
        const NApi::ITransactionPtr& transaction,
        TObjectId viewId,
        EMaterializedViewSourceType sourceType,
        TObjectId sourceObjectId,
        bool populate)
    {
        ProgressStore_->EnsureReady(transaction);

        auto partitionInfos = FetchPartitionInfos(
            client,
            transaction->GetId(),
            sourceType,
            FromObjectId(sourceObjectId),
            MasterReadOptions_);

        auto progress = New<TMaterializedViewProgress>();
        for (const auto& info : partitionInfos) {
            auto partition = New<TMaterializedViewPartitionProgress>();
            partition->ObjectId = info.ObjectId;
            partition->PartitionIndex = info.PartitionIndex;
            partition->NextRowIndex = populate ? 0 : info.RowCount;
            partition->TotalRowCount = info.RowCount;
            progress->Partitions.push_back(std::move(partition));
        }

        ProgressStore_->CreateProgress(transaction, viewId, std::move(progress));
        if (sourceType == EMaterializedViewSourceType::Queue) {
            ProgressStore_->CreateQueueConsumer(transaction, viewId);
        }
    }

private:
    THost* const Host_;
    const TCypressObjectRepositoryPtr Repository_;
    const TMaterializedViewsConfigPtr Config_;
    const NRpc::IChannelFactoryPtr ChannelFactory_;
    const TMaterializedViewProgressStorePtr ProgressStore_;
    const TMasterReadOptions MasterReadOptions_;
    const TActionQueuePtr ActionQueue_;
    const IInvokerPtr Invoker_;
    const TPeriodicExecutorPtr PeriodicExecutor_;
    const NProfiling::TCounter RefreshCounter_;

    std::optional<TTransactionId> LeadershipTransactionId_;
    THashMap<TObjectId, TMaterializedViewRefreshHandler> RefreshHandlers_;

    void ScanNonThrowing()
    {
        try {
            Scan();
        } catch (const std::exception& ex) {
            YT_TLOG_WARNING("Materialized view coordinator scan failed")
                .With(ex);
        }
    }

    void Scan()
    {
        auto leadershipTransactionId = Host_->GetLeadershipTransactionId();
        if (leadershipTransactionId != LeadershipTransactionId_) {
            RefreshHandlers_.clear();
            LeadershipTransactionId_ = leadershipTransactionId;
        }
        if (!leadershipTransactionId) {
            return;
        }

        auto views = Repository_->GetAllMaterializedViews();
        THashSet<TObjectId> viewIds;
        for (const auto& view : views) {
            if (Host_->GetLeadershipTransactionId() != leadershipTransactionId) {
                RefreshHandlers_.clear();
                LeadershipTransactionId_.reset();
                return;
            }
            viewIds.insert(view.ObjectId);

            auto [it, _] = RefreshHandlers_.try_emplace(
                view.ObjectId,
                Host_,
                ChannelFactory_,
                Config_,
                ProgressStore_,
                MasterReadOptions_,
                *leadershipTransactionId,
                RefreshCounter_);
            it->second.Execute(view);
        }

        DropMissingKeys(RefreshHandlers_, viewIds);
    }
};

////////////////////////////////////////////////////////////////////////////////

TMaterializedViewCoordinator::TMaterializedViewCoordinator(
    THost* host,
    TCypressObjectRepositoryPtr repository,
    TMaterializedViewsConfigPtr config,
    NRpc::IChannelFactoryPtr channelFactory)
    : Impl_(New<TImpl>(
        host,
        std::move(repository),
        std::move(config),
        std::move(channelFactory)))
{ }

TMaterializedViewCoordinator::~TMaterializedViewCoordinator() = default;

void TMaterializedViewCoordinator::Start()
{
    Impl_->Start();
}

void TMaterializedViewCoordinator::InitializeProgress(
    const NNative::IClientPtr& client,
    const NApi::ITransactionPtr& transaction,
    TObjectId viewId,
    EMaterializedViewSourceType sourceType,
    TObjectId sourceObjectId,
    bool populate)
{
    Impl_->InitializeProgress(
        client,
        transaction,
        viewId,
        sourceType,
        sourceObjectId,
        populate);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
