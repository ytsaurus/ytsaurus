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

const std::string LastErrorAttribute = "last_error";

struct TMaterializedViewProgress
    : public TYsonStruct
{
    i64 NextRowIndex;
    TInstant LastSuccessfulRefreshTime;

    REGISTER_YSON_STRUCT(TMaterializedViewProgress);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("next_row_index", &TThis::NextRowIndex)
            .Default(0);
        registrar.Parameter("last_successful_refresh_time", &TThis::LastSuccessfulRefreshTime)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TMaterializedViewProgress)
using TMaterializedViewProgressPtr = TIntrusivePtr<TMaterializedViewProgress>;

TMaterializedViewProgressPtr BuildProgress(i64 nextRowIndex, TInstant lastRefreshTime)
{
    auto progress = New<TMaterializedViewProgress>();
    progress->NextRowIndex = nextRowIndex;
    progress->LastSuccessfulRefreshTime = lastRefreshTime;
    return progress;
}

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

    void EnsureReady()
    {
        TCreateNodeOptions options;
        options.IgnoreExisting = true;
        options.Recursive = true;
        WaitFor(Client_->CreateNode(ProgressRootPath_, EObjectType::MapNode, options))
            .ThrowOnError();
    }

    void EnsureView(TObjectId viewId, i64 initialRowCount)
    {
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("value", BuildProgress(initialRowCount, /*lastRefreshTime*/ {}));
        attributes->Set(LastErrorAttribute, std::string());

        TCreateNodeOptions options;
        options.IgnoreExisting = true;
        options.Attributes = std::move(attributes);
        WaitFor(Client_->CreateNode(GetProgressNodePath(viewId), EObjectType::Document, options))
            .ThrowOnError();
    }

    TMaterializedViewProgressPtr GetProgress(const IClientBasePtr& client, TObjectId viewId) const
    {
        return ConvertTo<TMaterializedViewProgressPtr>(
            WaitFor(client->GetNode(GetProgressNodePath(viewId))).ValueOrThrow());
    }

    void SetProgress(
        const NApi::ITransactionPtr& transaction,
        TObjectId viewId,
        i64 nextRowIndex) const
    {
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("value", BuildProgress(nextRowIndex, TInstant::Now()));
        attributes->Set(LastErrorAttribute, std::string());
        WaitFor(transaction->MultisetAttributesNode(
            GetProgressNodePath(viewId) + "/@",
            attributes->ToMap()))
            .ThrowOnError();
    }

    void SetError(TObjectId viewId, const TError& error) const
    {
        WaitFor(Client_->SetNode(
            GetProgressNodePath(viewId) + "/@" + LastErrorAttribute,
            NYson::ConvertToYsonString(error.IsOK() ? std::string() : error.GetMessage())))
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
                YT_LOG_INFO("Materialized view refresh completed "
                    "(View: %v, InstanceCookie: %v, LowerRowIndex: %v, UpperRowIndex: %v)",
                    View_.ObjectName,
                    RefreshInstanceCookie_,
                    Task_.LowerRowIndex,
                    Task_.UpperRowIndex);
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
                YT_LOG_WARNING(abortError, "Failed to abort materialized view refresh transaction "
                    "(View: %v)", View_.ObjectName);
            }
            Transaction_.Reset();
        }

        if (ProgressReady_) {
            try {
                ProgressStore_->SetError(View_.ObjectId, error);
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Failed to persist materialized view refresh error "
                    "(View: %v)", View_.ObjectName);
            }
        }
    }

private:
    struct TRefreshTask
    {
        i64 LowerRowIndex = -1;
        i64 UpperRowIndex = -1;
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
    TRefreshTask Task_;
    bool ProgressReady_ = false;
    bool Active_ = false;
    bool Refreshed_ = false;
    int RefreshInstanceCookie_ = -1;

    void DoExecute()
    {
        ProgressStore_->EnsureView(View_.ObjectId, View_.InitialSourceRowCount);
        ProgressReady_ = true;
        Client_ = Host_->CreateClient(View_.Creator);

        TTransactionStartOptions options;
        options.Timeout = Config_->TransactionTimeout;
        Transaction_ = WaitFor(Client_->StartTransaction(ETransactionType::Master, options))
            .ValueOrThrow();

        Active_ = CollectLocks();
        if (!Active_) {
            return;
        }

        LoadRefreshState();
        ValidateRefreshState();
        auto task = BuildTask();
        if (!task) {
            return;
        }
        Task_ = std::move(*task);

        WaitFor(StartRefreshQuery(BuildRefreshQuery(Task_.LowerRowIndex, Task_.UpperRowIndex)))
            .ThrowOnError();
        THROW_ERROR_EXCEPTION_IF(Host_->GetConfig()->QuerySettings->Testing->ThrowExceptionAfterRefreshQuery,
            "Testing exception after materialized view refresh query");
        ProgressStore_->SetProgress(Transaction_, View_.ObjectId, Task_.UpperRowIndex);
        Refreshed_ = true;
    }

    bool CollectLocks()
    {
        auto progressLock = Transaction_->LockNode(
            ProgressStore_->GetProgressNodePath(View_.ObjectId),
            ELockMode::Exclusive);
        auto targetLock = Transaction_->LockNode(View_.TargetPath, ELockMode::Shared);
        auto mainLocksOrError = WaitFor(AllSucceeded(std::vector{progressLock, targetLock}));
        if (mainLocksOrError.FindMatching(NCypressClient::EErrorCode::ConcurrentTransactionLockConflict)) {
            return false;
        }
        auto mainLocks = std::move(mainLocksOrError).ValueOrThrow();
        TargetObjectId_ = mainLocks[1].NodeId;

        std::vector<TFuture<TLockNodeResult>> futures;
        futures.push_back(Transaction_->LockNode(FromObjectId(View_.ObjectId), ELockMode::Snapshot));
        futures.push_back(Transaction_->LockNode(View_.SourcePath, ELockMode::Snapshot));

        auto auxiliaryLocks = WaitFor(AllSucceeded(futures)).ValueOrThrow();
        SourceObjectId_ = auxiliaryLocks[1].NodeId;
        return true;
    }

    void LoadRefreshState()
    {
        SourceInfo_ = GetTableInfo(Transaction_, FromObjectId(SourceObjectId_), MasterReadOptions_);
        TargetInfo_ = GetTableInfo(Transaction_, FromObjectId(TargetObjectId_), MasterReadOptions_);
        Progress_ = ProgressStore_->GetProgress(Transaction_, View_.ObjectId);
    }

    void ValidateRefreshState() const
    {
        THROW_ERROR_EXCEPTION_IF(SourceObjectId_ != View_.SourceObjectId,
            "Materialized view source table was replaced")
            << TErrorAttribute("source_path", View_.SourcePath)
            << TErrorAttribute("expected_object_id", View_.SourceObjectId)
            << TErrorAttribute("actual_object_id", SourceObjectId_);
        THROW_ERROR_EXCEPTION_IF(TargetObjectId_ != View_.TargetObjectId,
            "Materialized view target table was replaced")
            << TErrorAttribute("target_path", View_.TargetPath)
            << TErrorAttribute("expected_object_id", View_.TargetObjectId)
            << TErrorAttribute("actual_object_id", TargetObjectId_);
        THROW_ERROR_EXCEPTION_IF(TargetInfo_.Dynamic,
            "Materialized view target table must be static")
            << TErrorAttribute("target_path", View_.TargetPath);
        THROW_ERROR_EXCEPTION_IF(SourceInfo_.Dynamic,
            "Materialized view source table must be static")
            << TErrorAttribute("source_path", View_.SourcePath);
        THROW_ERROR_EXCEPTION_IF(!SourceInfo_.RowCount,
            "Materialized view static source has no row count")
            << TErrorAttribute("source_path", View_.SourcePath);

        auto upperRowIndex = *SourceInfo_.RowCount;
        THROW_ERROR_EXCEPTION_IF(upperRowIndex < Progress_->NextRowIndex,
            "Materialized view source table is not append-only")
            << TErrorAttribute("source_path", View_.SourcePath)
            << TErrorAttribute("processed_row_count", Progress_->NextRowIndex)
            << TErrorAttribute("current_row_count", upperRowIndex);
    }

    std::optional<TRefreshTask> BuildTask() const
    {
        auto lowerRowIndex = Progress_->NextRowIndex;
        auto upperRowIndex = *SourceInfo_.RowCount;
        if (upperRowIndex == lowerRowIndex) {
            return std::nullopt;
        }
        if (Config_->MaxRowsPerRefresh > 0) {
            upperRowIndex = std::min(upperRowIndex, lowerRowIndex + Config_->MaxRowsPerRefresh);
        }
        return TRefreshTask{
            .LowerRowIndex = lowerRowIndex,
            .UpperRowIndex = upperRowIndex,
        };
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

        YT_LOG_INFO("Executing materialized view refresh query on clique instance "
            "(View: %v, InstanceId: %v, InstanceCookie: %v, Endpoint: %v)",
            View_.ObjectName,
            instanceId,
            RefreshInstanceCookie_,
            endpoint);

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
        ProgressStore_->EnsureReady();
        PeriodicExecutor_->Start();
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
            YT_LOG_WARNING(ex, "Materialized view coordinator scan failed");
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
                YT_LOG_WARNING(error, "Materialized view refresh failed (View: %v)", view.ObjectName);
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
