#include "chyt_engine.h"

#include "config.h"
#include "handler_base.h"

#include <yt/yt/ytlib/query_tracker_client/records/query.record.h>

#include <yt/chyt/client/query_service_proxy.h>

#include <yt/yt/library/clickhouse_discovery/config.h>
#include <yt/yt/library/clickhouse_discovery/discovery.h>
#include <yt/yt/library/clickhouse_discovery/discovery_v2.h>
#include <yt/yt/library/clickhouse_discovery/helpers.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/client/scheduler/public.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/caching_channel_factory.h>
#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/ytree/attributes.h>
#include <yt/yt/core/ytree/node.h>

namespace NYT::NQueryTracker {

using namespace NApi;
using namespace NYPath;
using namespace NHiveClient;
using namespace NQueryTrackerClient;
using namespace NClickHouseServer;
using namespace NRpc;
using namespace NRpc::NBus;
using namespace NYTree;
using namespace NConcurrency;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

class TChytSettings
    : public TYsonStruct
{
public:
    std::optional<std::string> Cluster;

    std::optional<TString> Clique;

    THashMap<TString, TString> QuerySettings;

    REGISTER_YSON_STRUCT(TChytSettings);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("cluster", &TThis::Cluster)
            .Default();
        registrar.Parameter("clique", &TThis::Clique)
            .Default();
        registrar.Parameter("query_settings", &TThis::QuerySettings)
            .Default();
        registrar.UnrecognizedStrategy(NYTree::EUnrecognizedStrategy::KeepRecursive);
    }
};

DEFINE_REFCOUNTED_TYPE(TChytSettings)
DECLARE_REFCOUNTED_CLASS(TChytSettings)

////////////////////////////////////////////////////////////////////////////////

class TChytQueryHandler
    : public TQueryHandlerBase
{
public:
    using TExecuteQueryResponse = TTypedClientResponse<NClickHouseServer::NProto::TRspExecuteQuery>::TResult;

    TChytQueryHandler(
        const IClientPtr& stateClient,
        const TYPath& stateRoot,
        const TChytEngineConfigPtr& config,
        const IChannelFactoryPtr& channelFactory,
        const NQueryTrackerClient::NRecords::TActiveQuery& activeQuery,
        const TClusterDirectoryPtr& clusterDirectory,
        const IInvokerPtr& controlInvoker)
        : TQueryHandlerBase(stateClient, stateRoot, controlInvoker, config, activeQuery)
        , Settings_(ConvertTo<TChytSettingsPtr>(SettingsNode_))
        , Config_(config)
        , Clique_(Settings_->Clique.value_or(config->DefaultClique))
        , Cluster_(Settings_->Cluster.value_or(config->DefaultCluster))
        , NativeConnection_(clusterDirectory->GetConnectionOrThrow(Cluster_))
        , QueryClient_(NativeConnection_->CreateClient(TClientOptions{.User = activeQuery.User}))
        , ChannelFactory_(channelFactory)
    { }

    void Start() override
    {
        YT_LOG_DEBUG("Starting CHYT query");
        OnQueryStarted();
        StartProgressWriter();

        AsyncQueryResult_ = BIND(&TChytQueryHandler::Execute, MakeStrong(this))
            .AsyncVia(GetCurrentInvoker())
            .Run();
        AsyncQueryResult_.Subscribe(BIND(&TChytQueryHandler::OnChytResponse, MakeWeak(this)).Via(GetCurrentInvoker()));
    }

    void Abort() override
    {
        // Nothing smarter than that for now.
        Cancelled_ = true;
        AsyncQueryResult_.Cancel(TError("Query aborted"));
    }

    void Detach() override
    {
        // Nothing smarter than that for now.
        Cancelled_ = true;
        AsyncQueryResult_.Cancel(TError("Query detached"));
    }

private:
    const TChytSettingsPtr Settings_;
    const TChytEngineConfigPtr Config_;
    TString Clique_;
    std::string Cluster_;
    NApi::NNative::IConnectionPtr NativeConnection_;
    NApi::IClientPtr QueryClient_;

    IChannelFactoryPtr ChannelFactory_;

    IDiscoveryPtr Discovery_;
    THashMap<TString, NYTree::IAttributeDictionaryPtr> Instances_;

    TFuture<TExecuteQueryResponse> AsyncQueryResult_;
    std::atomic<bool> Cancelled_ = false;
    bool IsProgressImplemented_ = true;

    static const inline std::vector<std::string> DiscoveryAttributes_ = std::vector<std::string>{
        "host",
        "rpc_port",
        "job_cookie",
        "clique_incarnation",
    };

    IChannelPtr GetChannelForRandomInstance()
    {
        auto instanceIterator = Instances_.begin();
        std::advance(instanceIterator, RandomNumber(Instances_.size()));

        auto attributes = instanceIterator->second;
        auto host = attributes->Get<TString>("host");
        auto rpcPort = attributes->Get<ui64>("rpc_port");
        return ChannelFactory_->CreateChannel(Format("%v:%v", host, rpcPort));
    }

    void CheckPermission()
    {
        YT_LOG_DEBUG("Checking permission");

        auto principalAclPath = Format("//sys/access_control_object_namespaces/chyt/%v/principal", Clique_);
        TCheckPermissionOptions options;
        options.ReadFrom = EMasterChannelKind::Cache;
        auto result = WaitFor(QueryClient_->CheckPermission(User_, principalAclPath, EPermission::Use, options))
            .ValueOrThrow();
        if (result.Action != NSecurityClient::ESecurityAction::Allow) {
            THROW_ERROR_EXCEPTION("User %Qv has no access to clique %Qv",
                User_,
                Clique_);
        }
    }

    void InitializeInstances()
    {
        YT_LOG_DEBUG("Initializing instances");

        Discovery_ = CreateDiscovery();
        WaitFor(Discovery_->UpdateList())
            .ThrowOnError();
        Instances_ = FilterInstancesByIncarnation(Discovery_->List());
    }

    IDiscoveryPtr CreateDiscovery()
    {
        YT_LOG_DEBUG("Getting discovery");

        auto config = New<TDiscoveryV2Config>();
        config->GroupId = Format("/chyt/%v", Clique_);
        config->ReadQuorum = 1;
        return CreateDiscoveryV2(
            std::move(config),
            NativeConnection_,
            ChannelFactory_,
            GetCurrentInvoker(),
            DiscoveryAttributes_,
            Logger);
    }

    TString GetStringRepresentation(const INodePtr& node)
    {
        switch (node->GetType()) {
            case ENodeType::Int64:
                return std::to_string(node->AsInt64()->GetValue());
            case ENodeType::Uint64:
                return std::to_string(node->AsUint64()->GetValue());
            case ENodeType::Double:
                return std::to_string(node->AsDouble()->GetValue());
            case ENodeType::Boolean:
                return node->AsBoolean()->GetValue() ? "1" : "0";
            case ENodeType::String:
                return node->AsString()->GetValue();
            default:
                THROW_ERROR_EXCEPTION("Can't convert non-scalar data to string (Type: %v)", node->GetType());
        }
    }

    void DFSForUnrecognizedSettings(
        THashMap<TString, TString>& flattenedSettings,
        const INodePtr& node)
    {
        auto type = node->GetType();
        switch (type) {
            case NYT::NYTree::ENodeType::Map:
                for (const auto& [_, unrecognized] : node->AsMap()->GetChildren()) {
                    DFSForUnrecognizedSettings(flattenedSettings, unrecognized);
                }
                break;
            default: {
                auto value = GetStringRepresentation(node);
                std::string path = node->GetPath();
                std::replace(path.begin(), path.end(), '/', '.');
                auto settingName = path.substr(1);
                auto [_, inserted] = flattenedSettings.emplace(settingName, value);
                if (!inserted) {
                    THROW_ERROR_EXCEPTION("Setting %v is present multiple times", settingName);
                }
                break;
            }
        }
    }

    THashMap<TString, TString> GetUnrecognizedFlattenedSettings()
    {
        auto unrecognized = Settings_->GetLocalUnrecognized();
        THashMap<TString, TString> flattenedSettings;
        DFSForUnrecognizedSettings(flattenedSettings, unrecognized);
        return flattenedSettings;
    }

    TExecuteQueryResponse Execute()
    {
        CheckPermission();
        InitializeInstances();

        auto instanceChannel = GetChannelForRandomInstance();
        TQueryServiceProxy proxy(instanceChannel);
        auto req = proxy.ExecuteQuery();

        SetAuthenticationIdentity(req, TAuthenticationIdentity(User_));
        req->set_row_count_limit(Config_->RowCountLimit);
        ToProto(req->mutable_query_id(), QueryId_);
        auto* chytRequest = req->mutable_chyt_request();
        chytRequest->set_query(Query_);

        for (const auto& [key, value] : GetUnrecognizedFlattenedSettings()) {
            auto [_, inserted] = Settings_->QuerySettings.emplace(key, value);
            if (!inserted) {
                THROW_ERROR_EXCEPTION("Setting %v is present multiple times", key);
            }
        }

        auto* settings = chytRequest->mutable_settings();
        for (const auto& [key, value] : Settings_->QuerySettings) {
            (*settings)[key] = value;
        }

        auto rsp = req->Invoke();

        auto progressPollerExecutor = New<TPeriodicExecutor>(
            GetCurrentInvoker(),
            BIND(&TChytQueryHandler::PollQueryProgress, MakeStrong(this), proxy, instanceChannel->GetEndpointDescription()),
            Config_->ProgressPollPeriod);
        progressPollerExecutor->Start();

        auto result = WaitFor(rsp);

        YT_UNUSED_FUTURE(progressPollerExecutor->Stop());

        return result.ValueOrThrow();
    }

    void PollQueryProgress(TQueryServiceProxy coordinator, std::string endpoint)
    {
        if (!IsProgressImplemented_) {
            return;
        }

        auto req = coordinator.GetQueryProgress();
        SetAuthenticationIdentity(req, TAuthenticationIdentity(User_));
        ToProto(req->mutable_query_id(), QueryId_);

        auto errorOrProgress = WaitFor(req->Invoke());
        if (errorOrProgress.IsOK()) {
            SetProgress(errorOrProgress.Value()->progress());
        } else {
            if (errorOrProgress.FindMatching(NRpc::EErrorCode::NoSuchMethod)) {
                IsProgressImplemented_ = false;
                return;
            }
            YT_LOG_ERROR(errorOrProgress, "Failed to get progress from coordinator (InstanceEndpoint: %v)", endpoint);
        }
    }

    TYsonString ProgressValuesToYsonString(const NClickHouseServer::NProto::TProgressValues& values)
    {
        return BuildYsonStringFluently()
            .BeginMap()
                .Item("read_rows").Value(values.read_rows())
                .Item("read_bytes").Value(values.read_bytes())
                .Item("total_rows_to_read").Value(values.total_rows_to_read())
                .Item("total_bytes_to_read").Value(values.total_bytes_to_read())
                .Item("finished").Value(values.finished())
            .EndMap();
    }

    void SetProgress(const NClickHouseServer::NProto::TQueryProgressValues& progress)
    {
        // ConvertToYsonString(progress)
        auto progressYson = BuildYsonStringFluently()
            .BeginMap()
                .Item("total_progress").Value(ProgressValuesToYsonString(progress.total_progress()))
                .Item("secondary_queries").DoMap([&] (TFluentMap fluent) {
                    for (int index = 0; index < progress.secondary_query_ids_size(); ++index) {
                        auto queryId = ToString(FromProto<TGuid>(progress.secondary_query_ids()[index]));
                        auto queryProgress = progress.secondary_query_progresses()[index];
                        fluent.Item(queryId).Value(ProgressValuesToYsonString(queryProgress));
                    }
                })
            .EndMap();

        OnProgress(std::move(progressYson));
    }

    void OnChytResponse(const TErrorOr<TExecuteQueryResponse>& rspOrError)
    {
        if (Cancelled_) {
            return;
        }
        if (!rspOrError.IsOK()) {
            OnQueryFailed(rspOrError);
            return;
        }
        const auto& rsp = rspOrError.Value();
        const auto& error = FromProto<TError>(rsp->error());
        if (!error.IsOK()) {
            OnQueryFailed(error);
            return;
        }

        std::vector<TErrorOr<TWireRowset>> wireRowsetOrErrors;
        wireRowsetOrErrors.reserve(rsp->Attachments().size());
        for (const auto& ref : rsp->Attachments()) {
            if (!ref.Empty()) {
                wireRowsetOrErrors.emplace_back(TWireRowset{.Rowset = ref});
            }
        }
        OnQueryCompletedWire(wireRowsetOrErrors);
    }
};

class TChytEngine
    : public IQueryEngine
{
public:
    TChytEngine(IClientPtr stateClient, TYPath stateRoot)
        : StateClient_(std::move(stateClient))
        , StateRoot_(std::move(stateRoot))
        , ControlQueue_(New<TActionQueue>("MockEngineControl"))
        , ClusterDirectory_(DynamicPointerCast<NNative::IConnection>(StateClient_->GetConnection())->GetClusterDirectory())
        , ChannelFactory_(CreateCachingChannelFactory(CreateTcpBusChannelFactory(New<NYT::NBus::TBusConfig>())))
    { }

    IQueryHandlerPtr StartOrAttachQuery(NRecords::TActiveQuery activeQuery) override
    {
        return New<TChytQueryHandler>(StateClient_, StateRoot_, ChytConfig_, ChannelFactory_, activeQuery, ClusterDirectory_, ControlQueue_->GetInvoker());
    }

    void Reconfigure(const TEngineConfigBasePtr& config) override
    {
        ChytConfig_ = DynamicPointerCast<TChytEngineConfig>(config);
    }

private:
    const IClientPtr StateClient_;
    const TYPath StateRoot_;
    const TActionQueuePtr ControlQueue_;
    TChytEngineConfigPtr ChytConfig_;
    TClusterDirectoryPtr ClusterDirectory_;
    IChannelFactoryPtr ChannelFactory_;
};

////////////////////////////////////////////////////////////////////////////////

IQueryEnginePtr CreateChytEngine(const IClientPtr& stateClient, const TYPath& stateRoot)
{
    return New<TChytEngine>(stateClient, stateRoot);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
