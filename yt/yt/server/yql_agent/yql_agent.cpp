#include "yql_agent.h"

#include "config.h"

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/yson/null_consumer.h>

#include <yql/library/embedded/yql_embedded.h>

namespace NYT::NYqlAgent {

using namespace NConcurrency;
using namespace NCypressElection;
using namespace NYTree;
using namespace NHiveClient;
using namespace NYqlClient;
using namespace NYqlClient::NProto;
using namespace NYson;

const auto& Logger = YqlAgentLogger;

////////////////////////////////////////////////////////////////////////////////

class TYqlAgent
    : public IYqlAgent
{
public:
    TYqlAgent(
        TYqlAgentConfigPtr config,
        TClusterDirectoryPtr clusterDirectory,
        IInvokerPtr controlInvoker,
        ICypressElectionManagerPtr electionManager,
        TString agentId)
        : Config_(std::move(config))
        , ClusterDirectory_(std::move(clusterDirectory))
        , ControlInvoker_(std::move(controlInvoker))
        , ElectionManager_(std::move(electionManager))
        , AgentId_(std::move(agentId))
        , ThreadPool_(New<TThreadPool>(Config_->YqlThreadCount, "Yql"))
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

        auto options = NYql::NEmbedded::TOperationFactoryOptions{
            .MrJobBinary_ = Config_->MRJobBinary,
            .LogLevel_ = ELogPriority::TLOG_DEBUG,
            .YtLogLevel_ = ILogger::DEBUG,
            .ResultFormat_ = NYson::EYsonFormat::Text,
            // .YtClusters_ = ytClusters,
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

    TFuture<TYqlResponse> StartQuery(TQueryId queryId, const TYqlRequest& request) override
    {
        YT_LOG_INFO("Starting query (QueryId: %v)", queryId);

        return BIND(&TYqlAgent::DoStartQuery, MakeStrong(this), queryId, request)
            .AsyncVia(ThreadPool_->GetInvoker())
            .Run();
    }

private:
    TYqlAgentConfigPtr Config_;
    TClusterDirectoryPtr ClusterDirectory_;
    IInvokerPtr ControlInvoker_;
    ICypressElectionManagerPtr ElectionManager_;
    TString AgentId_;

    std::unique_ptr<NYql::NEmbedded::IOperationFactory> OperationFactory_;

    TThreadPoolPtr ThreadPool_;

    TYqlResponse DoStartQuery(TQueryId queryId, const TYqlRequest& request)
    {
        const auto& Logger = YqlAgentLogger.WithTag("QueryId: %v", queryId);

        YT_LOG_INFO("Invoking YQL embedded");

        try {
            NYql::NEmbedded::TOperationOptions options;
            if (request.has_attributes()) {
                options.Attributes = request.attributes();
            }
            if (request.has_parameters()) {
                options.Parameters = request.parameters();
            }
            if (request.has_title()) {
                options.Title = request.title();
            }
            options.SyntaxVersion = request.syntax_version();
            options.Mode = static_cast<NYql::NEmbedded::EExecuteMode>(request.mode());
            // This is a long blocking call.
            auto operation = OperationFactory_->Run(request.query(), options);
            YT_LOG_INFO("YQL embedded call completed");

            TYqlResponse response;

            auto validateAndFillField = [&] (const TString& rawField, TString* (TYqlResponse::*mutableProtoFieldAccessor)()) {
                if (rawField.empty()) {
                    return;
                }
                // TODO(max42): original YSON tends to unnecessary pretty.
                *((&response)->*mutableProtoFieldAccessor)() = rawField;
            };

            validateAndFillField(operation->YsonResult(), &TYqlResponse::mutable_result);
            validateAndFillField(operation->Plan(), &TYqlResponse::mutable_plan);
            validateAndFillField(operation->Statistics(), &TYqlResponse::mutable_statistics);
            validateAndFillField(operation->TaskInfo(), &TYqlResponse::mutable_task_info);

            return response;
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
    IInvokerPtr controlInvoker,
    ICypressElectionManagerPtr electionManager,
    TString agentId)
{
    return New<TYqlAgent>(
        std::move(config),
        std::move(clusterDirectory),
        std::move(controlInvoker),
        std::move(electionManager),
        std::move(agentId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
