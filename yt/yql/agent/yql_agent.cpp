#include "yql_agent.h"

#include "config.h"
#include "interop.h"

#include <library/cpp/yt/logging/backends/arcadia/backend.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yql/plugin/bridge/plugin.h>


namespace NYT::NYqlAgent {

using namespace NConcurrency;
using namespace NYTree;
using namespace NHiveClient;
using namespace NYqlClient;
using namespace NYqlClient::NProto;
using namespace NYson;
using namespace NApi;
using namespace NHiveClient;
using namespace NSecurityClient;
using namespace NLogging;

const auto& Logger = YqlAgentLogger;

////////////////////////////////////////////////////////////////////////////////

class TYqlAgent
    : public IYqlAgent
{
public:
    TYqlAgent(
        TYqlAgentConfigPtr config,
        TClusterDirectoryPtr clusterDirectory,
        TClientDirectoryPtr clientDirectory,
        IInvokerPtr controlInvoker,
        TString agentId)
        : Config_(std::move(config))
        , ClusterDirectory_(std::move(clusterDirectory))
        , ClientDirectory_(std::move(clientDirectory))
        , ControlInvoker_(std::move(controlInvoker))
        , AgentId_(std::move(agentId))
        , ThreadPool_(CreateThreadPool(Config_->YqlThreadCount, "Yql"))
    {
        static const TYsonString EmptyMap = TYsonString(TString("{}"));

        auto operationAttributes = Config_->OperationAttributes
            ? ConvertToYsonString(Config_->OperationAttributes)
            : EmptyMap;

        THashMap<TString, TString> clusters;
        for (const auto& cluster : ClusterDirectory_->GetClusterNames()) {
            clusters[cluster] = cluster;
        }
        for (const auto& [cluster, address] : Config_->Clusters) {
            clusters[cluster] = address;
        }

        NYqlPlugin::TYqlPluginOptions options{
            .MRJobBinary = Config_->MRJobBinary,
            .UdfDirectory = Config_->UdfDirectory,
            .Clusters = clusters,
            .DefaultCluster = Config_->DefaultCluster,
            .OperationAttributes = operationAttributes,
            .YTTokenPath = Config_->YTTokenPath,
            .LogBackend = NYT::NLogging::CreateArcadiaLogBackend(TLogger("YqlPlugin")),
            .YqlPluginSharedLibrary = Config_->YqlPluginSharedLibrary,
        };
        YqlPlugin_ = NYqlPlugin::CreateYqlPlugin(options);
    }

    void Start() override
    { }

    void Stop() override
    { }

    NYTree::IMapNodePtr GetOrchidNode() const override
    {
        return GetEphemeralNodeFactory()->CreateMap();
    }

    void OnDynamicConfigChanged(
        const TYqlAgentDynamicConfigPtr& /*oldConfig*/,
        const TYqlAgentDynamicConfigPtr& /*newConfig*/) override
    { }

    TFuture<std::pair<TRspStartQuery, std::vector<TSharedRef>>> StartQuery(TQueryId queryId, const TString& impersonationUser, const TReqStartQuery& request) override
    {
        YT_LOG_INFO("Starting query (QueryId: %v, ImpersonationUser: %v)", queryId, impersonationUser);

        return BIND(&TYqlAgent::DoStartQuery, MakeStrong(this), queryId, impersonationUser, request)
            .AsyncVia(ThreadPool_->GetInvoker())
            .Run();
    }

private:
    const TYqlAgentConfigPtr Config_;
    const TClusterDirectoryPtr ClusterDirectory_;
    const TClientDirectoryPtr ClientDirectory_;
    const IInvokerPtr ControlInvoker_;
    const TString AgentId_;

    std::unique_ptr<NYqlPlugin::IYqlPlugin> YqlPlugin_;

    IThreadPoolPtr ThreadPool_;

    std::pair<TRspStartQuery, std::vector<TSharedRef>> DoStartQuery(TQueryId queryId, const TString& impersonationUser, const TReqStartQuery& request)
    {
        static const auto EmptyMap = TYsonString(TString("{}"));

        const auto& Logger = YqlAgentLogger.WithTag("QueryId: %v", queryId);

        const auto& yqlRequest = request.yql_request();

        TRspStartQuery response;

        YT_LOG_INFO("Invoking YQL embedded");

        std::vector<TSharedRef> wireRowsets;
        try {
            auto query = yqlRequest.query();
            if (request.build_rowsets()) {
                query = "pragma RefSelect; pragma yt.UseNativeYtTypes; " + query;
            }
            auto settings = yqlRequest.has_settings() ? TYsonString(yqlRequest.settings()) : EmptyMap;
            // This is a long blocking call.
            auto result = YqlPlugin_->Run(impersonationUser, query, settings);
            if (result.YsonError) {
                auto error = ConvertTo<TError>(TYsonString(*result.YsonError));
                THROW_ERROR error;
            }

            YT_LOG_INFO("YQL plugin call completed");

            TYqlResponse yqlResponse;

            auto validateAndFillField = [&] (const std::optional<TString>& rawField, TString* (TYqlResponse::*mutableProtoFieldAccessor)()) {
                if (!rawField) {
                    return;
                }
                // TODO(max42): original YSON tends to unnecessary pretty.
                *((&yqlResponse)->*mutableProtoFieldAccessor)() = *rawField;
            };

            validateAndFillField(result.YsonResult, &TYqlResponse::mutable_result);
            validateAndFillField(result.Plan, &TYqlResponse::mutable_plan);
            validateAndFillField(result.Statistics, &TYqlResponse::mutable_statistics);
            validateAndFillField(result.TaskInfo, &TYqlResponse::mutable_task_info);
            if (request.build_rowsets() && result.YsonResult) {
                auto rowsets = BuildRowsets(ClientDirectory_, *result.YsonResult, request.row_count_limit());
                for (const auto& rowset : rowsets) {
                    if (rowset.Error.IsOK()) {
                        wireRowsets.push_back(rowset.WireRowset);
                        response.add_rowset_errors();
                        response.add_incomplete(rowset.Incomplete);
                    } else {
                        wireRowsets.push_back(TSharedRef());
                        ToProto(response.add_rowset_errors(), rowset.Error);
                        response.add_incomplete(false);
                    }
                }
            }
            response.mutable_yql_response()->Swap(&yqlResponse);
            return {response, wireRowsets};
        } catch (const std::exception& ex) {
            auto error = TError("YQL plugin call failed") << TError(ex);
            YT_LOG_INFO(error, "YQL plugin call failed");
            THROW_ERROR error;
        }
    }
};

IYqlAgentPtr CreateYqlAgent(
    TYqlAgentConfigPtr config,
    TClusterDirectoryPtr clusterDirectory,
    TClientDirectoryPtr clientDirectory,
    IInvokerPtr controlInvoker,
    TString agentId)
{
    return New<TYqlAgent>(
        std::move(config),
        std::move(clusterDirectory),
        std::move(clientDirectory),
        std::move(controlInvoker),
        std::move(agentId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
