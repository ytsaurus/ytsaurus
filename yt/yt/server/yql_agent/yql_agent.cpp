#include "yql_agent.h"

#include "config.h"
#include "interop.h"

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <yql/library/embedded/yql_embedded.h>

#include <ads/bsyeti/libs/ytex/logging/adapters/global/global.h>

#include <ydb/library/yql/utils/log/log.h>

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
        // TODO(max42): Yql defines all standard clusters by itself.
//        TVector<NYql::TYtClusterOptions> ytClusters;
//        auto clusterNames = ClusterDirectory_->GetClusterNames();
//        Cerr << Format("%v", clusterNames) << Endl;
//        for (const auto& cluster : clusterNames) {
//            ytClusters.emplace_back(NYql::TYtClusterOptions{
//                .Name_ = cluster,
//                .Cluster_ = cluster,
//            });
//        }

        TVector<NYql::NEmbedded::TYtClusterOptions> ytClusters;
        for (const auto& cluster : Config_->AdditionalClusters) {
            ytClusters.emplace_back(NYql::NEmbedded::TYtClusterOptions{
                .Name_ = cluster,
                .Cluster_ = cluster,
            });
        }

        auto logBackend = NYTEx::NLogging::CreateLogBackend(TLogger("YqlEmbedded"));
        NYql::NLog::InitLogger(std::move(logBackend));

        auto options = NYql::NEmbedded::TOperationFactoryOptions{
            .MrJobBinary_ = Config_->MRJobBinary,
            .LogLevel_ = ELogPriority::TLOG_DEBUG,
            .YtLogLevel_ = ILogger::DEBUG,
            .ResultFormat_ = NYson::EYsonFormat::Text,
            .YtClusters_ = ytClusters,
            .YtToken_ = Config_->YTToken,
        };

        OperationFactory_.reset(NYql::NEmbedded::MakeOperationFactory(options).Release());
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

    TFuture<std::pair<TRspStartQuery, std::vector<TSharedRef>>> StartQuery(TQueryId queryId, const TReqStartQuery& request) override
    {
        YT_LOG_INFO("Starting query (QueryId: %v)", queryId);

        return BIND(&TYqlAgent::DoStartQuery, MakeStrong(this), queryId, request)
            .AsyncVia(ThreadPool_->GetInvoker())
            .Run();
    }

private:
    TYqlAgentConfigPtr Config_;
    TClusterDirectoryPtr ClusterDirectory_;
    TClientDirectoryPtr ClientDirectory_;
    IInvokerPtr ControlInvoker_;
    TString AgentId_;

    std::unique_ptr<NYql::NEmbedded::IOperationFactory> OperationFactory_;

    IThreadPoolPtr ThreadPool_;

    std::pair<TRspStartQuery, std::vector<TSharedRef>> DoStartQuery(TQueryId queryId, const TReqStartQuery& request)
    {
        const auto& Logger = YqlAgentLogger.WithTag("QueryId: %v", queryId);

        const auto& yqlRequest = request.yql_request();

        TRspStartQuery response;

        YT_LOG_INFO("Invoking YQL embedded");

        std::vector<TSharedRef> wireRowsets;
        try {
            NYql::NEmbedded::TOperationOptions options;
            if (yqlRequest.has_attributes()) {
                options.Attributes = yqlRequest.attributes();
            }
            if (yqlRequest.has_parameters()) {
                options.Parameters = yqlRequest.parameters();
            }
            if (yqlRequest.has_title()) {
                options.Title = yqlRequest.title();
            }
            options.SyntaxVersion = yqlRequest.syntax_version();
            options.Mode = static_cast<NYql::NEmbedded::EExecuteMode>(yqlRequest.mode());
            // This is a long blocking call.
            auto query = yqlRequest.query();
            if (request.build_rowsets()) {
                query = "pragma RefSelect; pragma yt.UseNativeYtTypes; " + query;
            }
            auto operation = OperationFactory_->Run(query, options);
            YT_LOG_INFO("YQL embedded call completed");

            TYqlResponse yqlResponse;

            auto validateAndFillField = [&] (const TString& rawField, TString* (TYqlResponse::*mutableProtoFieldAccessor)()) {
                if (rawField.empty()) {
                    return;
                }
                // TODO(max42): original YSON tends to unnecessary pretty.
                *((&yqlResponse)->*mutableProtoFieldAccessor)() = rawField;
            };

            validateAndFillField(operation->YsonResult(), &TYqlResponse::mutable_result);
            validateAndFillField(operation->Plan(), &TYqlResponse::mutable_plan);
            validateAndFillField(operation->Statistics(), &TYqlResponse::mutable_statistics);
            validateAndFillField(operation->TaskInfo(), &TYqlResponse::mutable_task_info);
            if (request.build_rowsets()) {
                auto rowsets = BuildRowsets(ClientDirectory_, operation->YsonResult(), request.row_count_limit());
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
            auto error = TError("YQL embedded call failed") << TError(ex);
            YT_LOG_INFO(error, "YQL embedded call failed");
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
