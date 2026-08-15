#include "materialized_view_coordinator.h"

#include "config.h"
#include "cypress_object_repository.h"
#include "host.h"

#include <yt/chyt/client/query_service_proxy.h>

#include <yt/yt/server/lib/misc/address_helpers.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/cypress_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/rpc/helpers.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

#include <util/random/random.h>

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

struct TMaterializedViewProgress
    : public TYsonStruct
{
    i64 NextRowIndex;
    std::optional<TInstant> LastSuccessfulRefreshTime;
    std::string LastError;

    REGISTER_YSON_STRUCT(TMaterializedViewProgress);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("next_row_index", &TThis::NextRowIndex)
            .Default(0);
        registrar.Parameter("last_successful_refresh_time", &TThis::LastSuccessfulRefreshTime)
            .Default();
        registrar.Parameter("last_error", &TThis::LastError)
            .Default();
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
    { }

    void EnsureReady(const IClientBasePtr& client)
    {
        TCreateNodeOptions options;
        options.IgnoreExisting = true;
        options.Recursive = true;
        WaitFor(client->CreateNode(ProgressRootPath_, EObjectType::MapNode, options))
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
        progress->LastError.clear();

        auto attributes = CreateEphemeralAttributes();
        attributes->Set("value", std::move(progress));

        WaitFor(transaction->MultisetAttributesNode(
            GetProgressNodePath(viewId) + "/@",
            attributes->ToMap()))
            .ThrowOnError();
    }

    void SetError(TObjectId viewId, const TError& error) const
    {
        auto lastError = error.IsOK() ? std::string() : error.GetMessage();
        WaitFor(Client_->SetNode(
            GetProgressNodePath(viewId) + "/last_error",
            NYson::ConvertToYsonString(lastError)))
            .ThrowOnError();
    }

    TYPath GetProgressNodePath(TObjectId viewId) const
    {
        return ProgressRootPath_ + "/" + ToYPathLiteral(ToString(viewId));
    }

private:
    const NNative::IClientPtr Client_;
    const TYPath ProgressRootPath_;
};

DECLARE_REFCOUNTED_CLASS(TMaterializedViewProgressStore)
DEFINE_REFCOUNTED_TYPE(TMaterializedViewProgressStore)

struct TTableInfo
{
    std::optional<i64> RowCount;
    bool Dynamic = false;
};

TTableInfo GetTableInfo(
    const IClientBasePtr& client,
    const TYPath& path,
    const TMasterReadOptions& masterReadOptions)
{
    TGetNodeOptions options;
    static_cast<TMasterReadOptions&>(options) = masterReadOptions;
    options.Attributes = {"dynamic", "row_count"};
    auto node = ConvertToNode(WaitFor(client->GetNode(path + "/@", options)).ValueOrThrow())->AsMap();
    return {
        .RowCount = node->FindChildValue<i64>("row_count"),
        .Dynamic = node->GetChildValueOrDefault<bool>("dynamic", false),
    };
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
        try {
            if (!Active_) {
                WaitFor(Transaction_->Abort()).ThrowOnError();
                Transaction_.Reset();
                return {};
            }

            WaitFor(Transaction_->Commit()).ThrowOnError();
            Transaction_.Reset();

            if (Refreshed_) {
                YT_TLOG_INFO("Materialized view refresh completed")
                    .With("View", View_.ObjectName)
                    .With("InstanceCookie", RefreshInstanceCookie_)
                    .With("NewRowIndex", Progress_->NextRowIndex);
            }
            return {};
        } catch (const std::exception& ex) {
            return TError(ex);
        }
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

        try {
            ProgressStore_->SetError(View_.ObjectId, error);
        } catch (const std::exception& ex) {
            YT_TLOG_WARNING("Failed to persist materialized view refresh error")
                .With("View", View_.ObjectName)
                .With(TError(ex));
        }
    }

private:
    struct TRefreshTask
    {
        i64 LowerRowIndex = 0;
        i64 UpperRowIndex = 0;

        bool IsEmpty() const
        {
            return LowerRowIndex == UpperRowIndex;
        }

        TMaterializedViewProgressPtr BuildUpdatedProgress(
            const TMaterializedViewProgressPtr& currentProgress) const
        {
            auto progress = CloneYsonStruct(currentProgress);
            progress->NextRowIndex = UpperRowIndex;
            progress->LastSuccessfulRefreshTime = TInstant::Now();
            return progress;
        }
    };

    THost* const Host_;
    const NRpc::IChannelFactoryPtr ChannelFactory_;
    const TMaterializedViewsConfigPtr Config_;
    const TMaterializedViewProgressStorePtr ProgressStore_;
    const TMasterReadOptions MasterReadOptions_;
    const TCypressObjectRepository::TMaterializedView View_;

    NNative::IClientPtr Client_;
    TObjectId SourceObjectId_;
    TObjectId TargetObjectId_;
    TTableInfo SourceInfo_;
    TTableInfo TargetInfo_;
    NApi::ITransactionPtr Transaction_;
    TMaterializedViewProgressPtr Progress_;
    bool Active_ = false;
    bool Refreshed_ = false;
    int RefreshInstanceCookie_ = -1;

    void DoExecute()
    {
        Client_ = Host_->CreateClient(View_.Creator);

        TTransactionStartOptions options;
        options.Timeout = Config_->TransactionTimeout;
        Transaction_ = WaitFor(Client_->StartTransaction(ETransactionType::Master, options))
            .ValueOrThrow();

        Active_ = TryLockViewForRefresh();
        if (!Active_) {
            return;
        }

        CollectSnapshotLocks();
        LoadRefreshState();
        ValidateRefreshState();

        auto task = BuildTask();
        if (task.IsEmpty()) {
            return;
        }

        WaitFor(StartRefreshQuery(BuildRefreshQuery(task.LowerRowIndex, task.UpperRowIndex)))
            .ThrowOnError();

        THROW_ERROR_EXCEPTION_IF(Host_->GetConfig()->QuerySettings->Testing->ThrowExceptionAfterRefreshQuery,
            "Testing exception after materialized view refresh query");

        auto updatedProgress = task.BuildUpdatedProgress(Progress_);
        ProgressStore_->SetProgress(
            Transaction_,
            View_.ObjectId,
            updatedProgress);

        Progress_ = std::move(updatedProgress);
        Refreshed_ = true;
    }

    bool TryLockViewForRefresh()
    {
        auto lockPath = ProgressStore_->GetProgressNodePath(View_.ObjectId);

        auto resultOrError = WaitFor(AllSucceeded(std::vector{
            Transaction_->LockNode(View_.TargetPath, ELockMode::Shared),
            Transaction_->LockNode(lockPath, ELockMode::Exclusive),
        }));

        if (resultOrError.FindMatching(NCypressClient::EErrorCode::ConcurrentTransactionLockConflict)) {
            return false;
        }

        TargetObjectId_ = resultOrError.ValueOrThrow()[0].NodeId;

        return true;
    }

    void CollectSnapshotLocks()
    {
        auto results = WaitFor(AllSucceeded(std::vector{
            Transaction_->LockNode(View_.SourcePath, ELockMode::Snapshot),
            Transaction_->LockNode(FromObjectId(View_.ObjectId), ELockMode::Snapshot),
        })).ValueOrThrow();

        SourceObjectId_ = results[0].NodeId;
    }

    void LoadRefreshState()
    {
        SourceInfo_ = GetTableInfo(Transaction_, FromObjectId(SourceObjectId_), MasterReadOptions_);
        TargetInfo_ = GetTableInfo(Transaction_, FromObjectId(TargetObjectId_), MasterReadOptions_);
        Progress_ = ProgressStore_->GetProgress(Transaction_, View_.ObjectId);
        THROW_ERROR_EXCEPTION_IF(!Progress_->LastSuccessfulRefreshTime,
            "Materialized view progress is not initialized")
            .With("view_id", View_.ObjectId);
    }

    void ValidateRefreshState() const
    {
        THROW_ERROR_EXCEPTION_IF(SourceObjectId_ != View_.SourceObjectId,
            "Materialized view source table was replaced")
            .With("source_path", View_.SourcePath)
            .With("expected_object_id", View_.SourceObjectId)
            .With("actual_object_id", SourceObjectId_);
        THROW_ERROR_EXCEPTION_IF(TargetObjectId_ != View_.TargetObjectId,
            "Materialized view target table was replaced")
            .With("target_path", View_.TargetPath)
            .With("expected_object_id", View_.TargetObjectId)
            .With("actual_object_id", TargetObjectId_);
        THROW_ERROR_EXCEPTION_IF(TargetInfo_.Dynamic,
            "Materialized view target table must be static")
            .With("target_path", View_.TargetPath);
        THROW_ERROR_EXCEPTION_IF(SourceInfo_.Dynamic,
            "Materialized view source table must be static")
            .With("source_path", View_.SourcePath);
        THROW_ERROR_EXCEPTION_IF(!SourceInfo_.RowCount,
            "Materialized view static source has no row count")
            .With("source_path", View_.SourcePath);

        auto upperRowIndex = *SourceInfo_.RowCount;
        THROW_ERROR_EXCEPTION_IF(upperRowIndex < Progress_->NextRowIndex,
            "Materialized view source table is not append-only")
            .With("source_path", View_.SourcePath)
            .With("processed_row_count", Progress_->NextRowIndex)
            .With("current_row_count", upperRowIndex);
    }

    TRefreshTask BuildTask() const
    {
        TRefreshTask task{
            .LowerRowIndex = Progress_->NextRowIndex,
            .UpperRowIndex = *SourceInfo_.RowCount,
        };

        if (Config_->MaxRowsPerRefresh > 0) {
            task.UpperRowIndex = std::min(task.UpperRowIndex, task.LowerRowIndex + Config_->MaxRowsPerRefresh);
        }

        return task;
    }

    std::string BuildRefreshQuery(
        i64 lowerRowIndex,
        i64 upperRowIndex) const
    {
        TRichYPath rangedSourcePath(FromObjectId(SourceObjectId_));

        TReadLimit lower;
        lower.SetRowIndex(lowerRowIndex);
        TReadLimit upper;
        upper.SetRowIndex(upperRowIndex);
        rangedSourcePath.SetRanges({TReadRange(std::move(lower), std::move(upper))});

        const auto& createQuery = View_.CreateQuery->as<const DB::ASTCreateQuery&>();
        auto insertQuery = std::make_shared<DB::ASTInsertQuery>();
        insertQuery->table_id.table_name = FromObjectId(TargetObjectId_);
        insertQuery->select = createQuery.select->clone();
        insertQuery->children.push_back(insertQuery->select);
        auto& selectWithUnion = insertQuery->select->as<DB::ASTSelectWithUnionQuery&>();
        for (auto& select : selectWithUnion.list_of_selects->children) {
            select->as<DB::ASTSelectQuery&>().replaceDatabaseAndTable(
                /*databaseName*/ {}, ToString(rangedSourcePath));
        }
        return insertQuery->formatWithSecretsOneLine();
    }

    TFuture<void> StartRefreshQuery(const std::string& query)
    {
        auto instances = Host_->GetDiscoveryNodes();
        THROW_ERROR_EXCEPTION_IF(instances.empty(),
            "Cannot execute query since there are no active clique instances");

        auto instanceIt = instances.begin();
        std::advance(instanceIt, RandomNumber<size_t>(instances.size()));
        const auto& [instanceId, attributes] = *instanceIt;
        RefreshInstanceCookie_ = attributes->Get<int>("job_cookie");
        auto endpoint = NNet::BuildServiceAddress(
            attributes->Get<TString>("host"),
            attributes->Get<int>("rpc_port"));

        YT_TLOG_INFO("Executing materialized view refresh query on clique instance")
            .With("View", View_.ObjectName)
            .With("InstanceId", instanceId)
            .With("InstanceCookie", RefreshInstanceCookie_)
            .With("Endpoint", endpoint);

        TQueryServiceProxy proxy(ChannelFactory_->CreateChannel(endpoint));
        proxy.SetDefaultTimeout(Config_->QueryTimeout);

        auto req = proxy.ExecuteQuery();
        NRpc::SetAuthenticationIdentity(req, NRpc::TAuthenticationIdentity(View_.Creator));
        ToProto(req->mutable_query_id(), TQueryId::Create());
        ToProto(req->mutable_parent_transaction_id(), Transaction_->GetId());
        req->mutable_chyt_request()->set_query(query);

        return req->Invoke().Apply(BIND([] (const TQueryServiceProxy::TRspExecuteQueryPtr& rsp) {
            FromProto<TError>(rsp->error()).ThrowOnError();
        }));
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
        const NApi::ITransactionPtr& transaction,
        TObjectId viewId,
        TObjectId sourceObjectId)
    {
        ProgressStore_->EnsureReady(transaction);
        auto sourceInfo = GetTableInfo(transaction, FromObjectId(sourceObjectId), MasterReadOptions_);

        auto progress = New<TMaterializedViewProgress>();
        progress->NextRowIndex = sourceInfo.RowCount.value_or(0);
        progress->LastSuccessfulRefreshTime = TInstant::Now();
        ProgressStore_->CreateProgress(transaction, viewId, std::move(progress));
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
                .With(TError(ex));
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
    const NApi::ITransactionPtr& transaction,
    TObjectId viewId,
    TObjectId sourceObjectId)
{
    Impl_->InitializeProgress(transaction, viewId, sourceObjectId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
