#include "materialized_view_coordinator.h"

#include "config.h"
#include "cypress_object_repository.h"
#include "helpers.h"
#include "host.h"
#include "storage_yt_materialized_view.h"

#include <yt/chyt/client/query_service_proxy.h>

#include <yt/yt/server/lib/misc/address_helpers.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/cypress_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/queue_client/consumer_client.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler.h>

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
    TObjectId ObjectId{};
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

DEFINE_REFCOUNTED_TYPE(TMaterializedViewPartitionProgress)
using TMaterializedViewPartitionProgressPtr = TIntrusivePtr<TMaterializedViewPartitionProgress>;

struct TMaterializedViewProgress
    : public TYsonStruct
{
    std::vector<TMaterializedViewPartitionProgressPtr> Partitions;
    std::string LastError;
    bool QueueConsumerInitialized = false;

    REGISTER_YSON_STRUCT(TMaterializedViewProgress);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("partitions", &TThis::Partitions);
        registrar.Parameter("last_error", &TThis::LastError)
            .Default();
        registrar.Parameter("queue_consumer_initialized", &TThis::QueueConsumerInitialized)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TMaterializedViewProgress)
using TMaterializedViewProgressPtr = TIntrusivePtr<TMaterializedViewProgress>;

class TMaterializedViewProgressStore
    : public TRefCounted
{
public:
    TMaterializedViewProgressStore(
        NNative::IClientPtr client,
        TYPath rootPath)
        : Client_(std::move(client))
        , ProgressRootPath_(rootPath + "/progress")
        , ConsumerRootPath_(rootPath + "/consumers")
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

    TMaterializedViewProgressPtr GetProgress(const IClientBasePtr& client, TObjectId viewId) const
    {
        return ConvertTo<TMaterializedViewProgressPtr>(
            WaitFor(client->GetNode(GetProgressNodePath(viewId))).ValueOrThrow());
    }

    void CreateProgress(
        const NApi::ITransactionPtr& transaction,
        TObjectId viewId,
        TMaterializedViewProgressPtr progress) const
    {
        progress->LastError.clear();

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
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("value", std::move(progress));

        WaitFor(transaction->MultisetAttributesNode(
            GetProgressNodePath(viewId) + "/@",
            attributes->ToMap()))
            .ThrowOnError();
    }

    TFuture<void> SetError(TObjectId viewId, const TError& error) const
    {
        auto lastError = error.IsOK() ? std::string() : error.GetMessage();
        return Client_->SetNode(
            GetProgressNodePath(viewId) + "/last_error",
            NYson::ConvertToYsonString(lastError));
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

    NQueueClient::ISubConsumerClientPtr PrepareQueueConsumer(
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

        return NQueueClient::CreateSubConsumerClient(client, client, consumerPath, queuePath);
    }

    TYPath GetProgressNodePath(TObjectId viewId) const
    {
        return ProgressRootPath_ + "/" + ToYPathLiteral(ToString(viewId));
    }

private:
    const NNative::IClientPtr Client_;
    const TYPath ProgressRootPath_;
    const TYPath ConsumerRootPath_;

    TYPath GetConsumerPath(TObjectId viewId) const
    {
        return ConsumerRootPath_ + "/" + ToYPathLiteral(ToString(viewId));
    }
};

DECLARE_REFCOUNTED_CLASS(TMaterializedViewProgressStore)
DEFINE_REFCOUNTED_TYPE(TMaterializedViewProgressStore)

struct TPartitionInfo
{
    TObjectId ObjectId{};
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
                "Materialized view table range contains a dynamic table")
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

class TMaterializedViewRefreshContext
{
public:
    TMaterializedViewRefreshContext(
        THost* host,
        NRpc::IChannelFactoryPtr channelFactory,
        TMaterializedViewsConfigPtr config,
        TMaterializedViewProgressStorePtr progressStore,
        TMasterReadOptions masterReadOptions,
        TCypressObjectRepository::TMaterializedView view)
        : Host_(host)
        , ChannelFactory_(std::move(channelFactory))
        , Config_(std::move(config))
        , ProgressStore_(std::move(progressStore))
        , MasterReadOptions_(std::move(masterReadOptions))
        , View_(std::move(view))
    { }

    TError Execute()
    {
        try {
            DoExecute();
            return {};
        } catch (const std::exception& ex) {
            return TError(ex);
        }
    }

    TError Commit()
    {
        TError error;
        if (!NeedCommit_) {
            error = WaitFor(Transaction_->Abort());
        } else {
            error = WaitFor(Transaction_->Commit());
        }

        if (!error.IsOK()) {
            return error;
        }
        Transaction_.Reset();

        if (!Refreshed_) {
            return {};
        }

        const auto& testingConfig = Host_->GetConfig()->QuerySettings->Testing;
        try {
            if (auto breakpointFilename = testingConfig->MaterializedViewConsumerCommitBreakpoint) {
                HandleBreakpoint(*breakpointFilename, Host_->GetRootClient());
            }
        } catch (const std::exception& ex) {
            return TError(ex);
        }

        if (testingConfig->ThrowExceptionAfterRefreshCommit) {
            return TError("Testing exception after materialized view refresh transaction commit");
        }

        error = CommitConsumerPersistedOffsets();
        if (!error.IsOK()) {
            return error;
        }

        YT_TLOG_INFO("Materialized view refresh completed")
            .With("View", View_.ObjectName)
            .With("SuccessfulPartitionCount", SuccessCount_);
        return {};
    }

    void Abort(const TError& error)
    {
        if (Transaction_) {
            auto abortError = WaitFor(Transaction_->Abort());
            if (!abortError.IsOK()) {
                YT_TLOG_WARNING("Failed to abort materialized view refresh transaction")
                    .With("View", View_.ObjectName)
                    .With(abortError);
            }
            Transaction_.Reset();
        }

        auto failError = WaitFor(ProgressStore_->SetError(View_.ObjectId, error));
        if (!failError.IsOK()) {
            YT_TLOG_WARNING("Failed to persist materialized view refresh error")
                .With("View", View_.ObjectName)
                .With(failError);
        }
    }

private:
    struct TRefreshTask
    {
        TObjectId ObjectId{};
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

    THost* const Host_;
    const NRpc::IChannelFactoryPtr ChannelFactory_;
    const TMaterializedViewsConfigPtr Config_;
    const TMaterializedViewProgressStorePtr ProgressStore_;
    const TMasterReadOptions MasterReadOptions_;
    const TCypressObjectRepository::TMaterializedView View_;

    NNative::IClientPtr Client_;
    NApi::ITransactionPtr Transaction_;
    NQueueClient::ISubConsumerClientPtr SubConsumerClient_;
    std::vector<TRefreshResult> RefreshResults_;
    bool NeedCommit_ = false;
    bool NeedProgressFlush_ = false;
    bool Refreshed_ = false;

    i64 SuccessCount_ = 0;

    void DoExecute()
    {
        Client_ = Host_->CreateClient(View_.Creator);

        TTransactionStartOptions options;
        options.Timeout = Config_->TransactionTimeout;
        Transaction_ = WaitFor(Client_->StartTransaction(ETransactionType::Master, options))
            .ValueOrThrow();

        auto refreshLockPath = ProgressStore_->GetProgressNodePath(View_.ObjectId);
        auto refreshLockOrError = WaitFor(Transaction_->LockNode(refreshLockPath, ELockMode::Exclusive));
        if (refreshLockOrError.FindMatching(NCypressClient::EErrorCode::ConcurrentTransactionLockConflict)) {
            return;
        }
        refreshLockOrError.ThrowOnError();

        auto viewLocks = WaitFor(AllSucceeded(std::vector{
            Transaction_->LockNode(View_.SourcePath, ELockMode::Snapshot),
            Transaction_->LockNode(View_.TargetPath, ELockMode::Shared),
        })).ValueOrThrow();

        auto sourceObjectId = viewLocks[0].NodeId;
        auto targetObjectId = viewLocks[1].NodeId;

        ValidateTarget(targetObjectId);

        auto persistedProgress = ProgressStore_->GetProgress(Transaction_, View_.ObjectId);
        if (View_.SourceType == EMaterializedViewSourceType::Queue) {
            SubConsumerClient_ = ProgressStore_->PrepareQueueConsumer(
                Client_,
                View_.ObjectId,
                View_.SourcePath,
                persistedProgress->QueueConsumerInitialized,
                Config_->TableMountTimeout);

            NeedProgressFlush_ = !persistedProgress->QueueConsumerInitialized;
            persistedProgress->QueueConsumerInitialized = true;
        }

        auto partitionInfos = FetchPartitionInfos(
            Client_,
            Transaction_->GetId(),
            View_.SourceType,
            FromObjectId(sourceObjectId),
            MasterReadOptions_);

        auto currentProgress = BuildCurrentProgress(persistedProgress, partitionInfos);

        if (View_.SourceType == EMaterializedViewSourceType::Queue) {
            RecoverConsumerOffsetsIfNeeded(currentProgress);
        }

        auto tasks = BuildTasks(currentProgress);

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

        auto results = WaitFor(RunRefreshTasks(targetObjectId, tasks)).ValueOrThrow();
        for (const auto& result : results) {
            result.Result.ThrowOnError();
        }
        if (!results.empty()) {
            THROW_ERROR_EXCEPTION_IF(Host_->GetConfig()->QuerySettings->Testing->ThrowExceptionAfterRefreshQuery,
                "Testing exception after materialized view refresh query");
        }

        UpdateProgress(currentProgress, results);

        if (NeedProgressFlush_) {
            ProgressStore_->SetProgress(
                Transaction_,
                View_.ObjectId,
                currentProgress);
            NeedCommit_ = true;
            RefreshResults_ = std::move(results);
        }

        Refreshed_ = !tasks.empty();
    }

    void ValidateTarget(TObjectId targetId) const
    {
        TGetNodeOptions options;
        static_cast<TMasterReadOptions&>(options) = MasterReadOptions_;
        options.Attributes = {"type", "dynamic"};
        auto node = ConvertToNode(WaitFor(Transaction_->GetNode(FromObjectId(targetId) + "/@", options))
            .ValueOrThrow())->AsMap();

        if (node->GetChildValueOrThrow<EObjectType>("type") != EObjectType::Table) {
            THROW_ERROR_EXCEPTION("Materialized view target table must be a table")
                .With("target_path", View_.TargetPath);
        }

        if (node->GetChildValueOrDefault<bool>("dynamic", false)) {
            THROW_ERROR_EXCEPTION("Materialized view target table must be static")
                .With("target_path", View_.TargetPath);
        }
    }

    TMaterializedViewProgressPtr BuildCurrentProgress(
        const TMaterializedViewProgressPtr& persistedProgress,
        const std::vector<TPartitionInfo>& partitionInfos)
    {
        auto progress = New<TMaterializedViewProgress>();
        progress->LastError = persistedProgress->LastError;
        progress->QueueConsumerInitialized = persistedProgress->QueueConsumerInitialized;
        progress->Partitions.reserve(partitionInfos.size());

        THashMap<std::pair<TObjectId, std::optional<int>>, TMaterializedViewPartitionProgressPtr> partitionProgresses;
        for (const auto& partition : persistedProgress->Partitions) {
            partitionProgresses.emplace(
                std::pair(partition->ObjectId, partition->PartitionIndex),
                partition);
        }

        bool changed = persistedProgress->Partitions.size() != partitionInfos.size();
        for (const auto& info : partitionInfos) {
            TMaterializedViewPartitionProgressPtr partition;
            auto key = std::pair(info.ObjectId, info.PartitionIndex);
            if (auto it = partitionProgresses.find(key); it != partitionProgresses.end()) {
                partition = CloneYsonStruct(it->second);
            } else {
                changed = true;
                partition = New<TMaterializedViewPartitionProgress>();
                partition->ObjectId = info.ObjectId;
                partition->PartitionIndex = info.PartitionIndex;
            }

            THROW_ERROR_EXCEPTION_IF(info.RowCount < partition->TotalRowCount,
                "Partition is not append-only")
                .With("object_id", info.ObjectId)
                .With("partition_index", info.PartitionIndex);

            if (partition->TotalRowCount != info.RowCount) {
                changed = true;
            }
            partition->TotalRowCount = info.RowCount;
            progress->Partitions.push_back(std::move(partition));
        }

        NeedProgressFlush_ |= changed;

        return progress;
    }

    std::vector<TRefreshTask> BuildTasks(const TMaterializedViewProgressPtr& currentProgress) const
    {
        std::vector<TRefreshTask> tasks;
        tasks.reserve(currentProgress->Partitions.size());
        for (int index = 0; index < std::ssize(currentProgress->Partitions); ++index) {
            const auto& progress = currentProgress->Partitions[index];
            if (progress->NextRowIndex == progress->TotalRowCount) {
                continue;
            }

            auto lowerRowIndex = progress->NextRowIndex;
            auto upperRowIndex = progress->TotalRowCount;
            if (Config_->MaxRowsPerRefresh > 0) {
                upperRowIndex = std::min(upperRowIndex, lowerRowIndex + Config_->MaxRowsPerRefresh);
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

    void UpdateProgress(
        const TMaterializedViewProgressPtr& progress,
        const std::vector<TRefreshResult>& results)
    {
        auto now = TInstant::Now();

        for (const auto& result : results) {
            auto& partition = progress->Partitions[result.PartitionIndex];
            ++SuccessCount_;
            partition->LastUpdate = now;
            partition->LastError.clear();
            partition->NextRowIndex = result.Result.Value();
        }
        NeedProgressFlush_ = NeedProgressFlush_ || !results.empty();
        if (!progress->LastError.empty()) {
            progress->LastError.clear();
            NeedProgressFlush_ = true;
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
        auto& select = selectWithUnion.list_of_selects->children[0]->as<DB::ASTSelectQuery&>();
        auto* tableExpression = GetSingleTableExpression(&select);
        if (!tableExpression) {
            THROW_ERROR_EXCEPTION("Materialized view SELECT has malformed table expression");
        }

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

    TFuture<std::vector<TRefreshResult>> RunRefreshTasks(
        TObjectId targetObjectId,
        const std::vector<TRefreshTask>& tasks)
    {
        std::vector<TRefreshResult> results;
        results.reserve(tasks.size());
        std::vector<TFuture<void>> taskFutures;
        taskFutures.reserve(tasks.size());

        auto queries = BuildRefreshQueries(targetObjectId, tasks);
        for (int index = 0; index < std::ssize(tasks); ++index) {
            taskFutures.push_back(StartRefreshQuery(queries[index]));

            const auto& task = tasks[index];
            results.push_back({
                .PartitionIndex = task.PartitionIndex,
                .OldOffset = task.LowerRowIndex,
                .Result = task.UpperRowIndex,
            });
        }

        return AllSet(std::move(taskFutures))
            .AsUnique()
            .Apply(BIND([
                results = std::move(results)
            ] (std::vector<TError>&& errors) mutable {
                for (int index = 0; index < std::ssize(results); ++index) {
                    auto& result = results[index];
                    if (!errors[index].IsOK()) {
                        result.Result = std::move(errors[index]);
                    }
                }
                return std::move(results);
            }));
    }

    TFuture<void> StartRefreshQuery(const std::string& query)
    {
        auto instances = Host_->GetDiscoveryNodes();
        if (instances.empty()) {
            return MakeFuture<void>(TError("Cannot execute query since there are no active clique instances"));
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

        TQueryServiceProxy proxy(ChannelFactory_->CreateChannel(endpoint));
        proxy.SetDefaultTimeout(Config_->QueryTimeout);

        auto req = proxy.ExecuteQuery();
        NRpc::SetAuthenticationIdentity(req, NRpc::TAuthenticationIdentity(View_.Creator));
        ToProto(req->mutable_query_id(), TQueryId::Create());
        ToProto(req->mutable_parent_transaction_id(), Transaction_->GetId());
        auto* chytRequest = req->mutable_chyt_request();
        chytRequest->set_query(query);
        if (View_.SourceType == EMaterializedViewSourceType::Queue) {
            (*chytRequest->mutable_settings())["chyt.dynamic_table.enable_dynamic_store_read"] = "1";
        }

        return req->Invoke().Apply(BIND([] (const TQueryServiceProxy::TRspExecuteQueryPtr& rsp) {
            FromProto<TError>(rsp->error()).ThrowOnError();
        }));
    }

    void RecoverConsumerOffsetsIfNeeded(const TMaterializedViewProgressPtr& progress)
    {
        auto partitionCount = std::ssize(progress->Partitions);
        auto partitionInfos = WaitFor(SubConsumerClient_->CollectPartitions(partitionCount))
            .ValueOrThrow();
        std::vector<std::pair<int, i64>> staleOffsets;
        for (const auto& info : partitionInfos) {
            auto nextRowIndex = progress->Partitions[info.PartitionIndex]->NextRowIndex;
            if (info.NextRowIndex < nextRowIndex) {
                staleOffsets.emplace_back(info.PartitionIndex, nextRowIndex);
            }
        }
        if (staleOffsets.empty()) {
            return;
        }
        auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
            .ValueOrThrow();
        for (auto [partitionIndex, newOffset] : staleOffsets) {
            SubConsumerClient_->Advance(transaction, partitionIndex, /*oldOffset*/ std::nullopt, newOffset);
        }
        WaitFor(transaction->Commit()).ThrowOnError();
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
                SubConsumerClient_->Advance(transaction, result.PartitionIndex, result.OldOffset, result.Result.Value());
            }
            WaitFor(transaction->Commit()).ThrowOnError();
            return {};
        } catch (const std::exception& ex) {
            return TError(ex);
        }
    }
};

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
        , ProgressStore_(New<TMaterializedViewProgressStore>(Host_->GetRootClient(), Config_->RootPath))
        , MasterReadOptions_(*Host_->GetConfig()->TableAttributeCache->MasterReadOptions)
        , ActionQueue_(New<TActionQueue>("MaterializedViews"))
        , Invoker_(ActionQueue_->GetInvoker())
        , PeriodicExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TImpl::ScanNonThrowing, MakeWeak(this)),
            Config_->ScanPeriod))
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
        TObjectId sourceObjectId)
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
            partition->NextRowIndex = info.RowCount;
            partition->TotalRowCount = partition->NextRowIndex;
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
        if (!Host_->IsLeader()) {
            return;
        }

        for (const auto& view : Repository_->GetAllMaterializedViews()) {
            if (!Host_->IsLeader()) {
                return;
            }
            auto error = RefreshView(view);
            if (!error.IsOK()) {
                YT_TLOG_WARNING("Materialized view refresh failed")
                    .With("View", view.ObjectName)
                    .With(error);
            }
        }
    }

    TError RefreshView(const TCypressObjectRepository::TMaterializedView& view)
    {
        TMaterializedViewRefreshContext context(
            Host_,
            ChannelFactory_,
            Config_,
            ProgressStore_,
            MasterReadOptions_,
            view);

        auto error = context.Execute();
        if (error.IsOK()) {
            error = context.Commit();
        }
        if (!error.IsOK()) {
            context.Abort(error);
        }
        return error;
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
    TObjectId sourceObjectId)
{
    Impl_->InitializeProgress(
        client,
        transaction,
        viewId,
        sourceType,
        sourceObjectId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
