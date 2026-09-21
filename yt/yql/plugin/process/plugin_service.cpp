#include "plugin_service.h"

#include <yt/yt/core/rpc/service_detail.h>
#include <yt/yt/ytlib/yql_plugin/yql_plugin_proxy.h>

namespace NYT::NYqlPlugin::NProcess {

YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, YqlPluginServiceLogger, "YqlPluginService");


using namespace NConcurrency;
using namespace NRpc;
using namespace NYson;

using NYqlClient::NProto::TYqlQueryFile;
using NYqlClient::NProto::TYqlResponse;

////////////////////////////////////////////////////////////////////////////////

template <class TResponse, class TContext>
void ReplyClustersResult(
    TQueryId queryId,
    const TClustersResult& result,
    const TResponse& response,
    const TContext& context)
{
    for (const auto& [clusterName, clusterAddress] : result.Clusters) {
        auto* cluster = response->add_clusters();
        cluster->set_cluster_name(clusterName);
        cluster->set_cluster_address(clusterAddress);
    }
    if (result.DefaultCluster) {
        response->set_default_cluster(*result.DefaultCluster);
    }
    if (result.YsonError) {
        response->set_error(*result.YsonError);
    }

    context->AnnotateResponse()
        .With("QueryId", queryId)
        .With("Clusters", result.Clusters)
        .With("DefaultCluster", result.DefaultCluster)
        .With("Error", result.YsonError);
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

class TYqlPluginService
    : public TServiceBase
{
public:
    TYqlPluginService(IInvokerPtr controlInvoker, std::unique_ptr<IYqlPlugin> yqlPlugin)
        : TServiceBase(
            std::move(controlInvoker),
            TYqlPluginProxy::GetDescriptor(),
            YqlPluginServiceLogger())
        , YqlPlugin_(std::move(yqlPlugin))
        , QueryActionQueue_(New<TActionQueue>("QueryRunner"))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RunQuery)
            .SetCancelable(true)
            // Run in separate thread because RunQuery is long blocking call
            .SetInvoker(QueryActionQueue_->GetInvoker()));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortQuery));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetQueryProgress));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetUsedClusters));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetClustersInfo));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetDeclaredParametersInfo));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterQuery));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UnregisterQuery));
    }

    DECLARE_RPC_SERVICE_METHOD(NYqlPlugin::NProto, RunQuery)
    {
        auto queryId = FromProto<TQueryId>(request->query_id());
        context->AnnotateRequest()
            .With("QueryId", queryId)
            .With("User", request->user())
            .With("ExecuteMode", request->mode());

        auto files = ExtractFiles(request->files());

        auto queryResult = YqlPlugin_->Run(
          queryId,
          request->user(),
          request->query_identity_token(),
          TYsonString(request->credentials()),
          request->query_text(),
          TYsonString(request->settings()),
          std::move(files),
          request->mode(),
          FromProto<NYqlClient::EQueryType>(request->query_type()));

        auto yqlResponse = ToYqlResponse(queryResult);

        response->mutable_response()->Swap(&yqlResponse);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NYqlPlugin::NProto, GetUsedClusters)
    {
        auto queryId = FromProto<TQueryId>(request->query_id());
        context->AnnotateRequest()
            .With("QueryId", queryId);

        auto files = ExtractFiles(request->files());
        auto result = YqlPlugin_->GetUsedClusters(
          queryId,
          request->query_text(),
          TYsonString(request->settings()),
          files);
        ReplyClustersResult(queryId, result, response, context);
    }

    DECLARE_RPC_SERVICE_METHOD(NYqlPlugin::NProto, GetClustersInfo)
    {
        auto queryId = FromProto<TQueryId>(request->query_id());
        context->AnnotateRequest()
            .With("QueryId", queryId);

        auto result = YqlPlugin_->GetClustersInfo(queryId);
        ReplyClustersResult(queryId, result, response, context);
    }

    DECLARE_RPC_SERVICE_METHOD(NYqlPlugin::NProto, AbortQuery)
    {
        auto queryId = FromProto<TQueryId>(request->query_id());

        context->AnnotateRequest()
            .With("QueryId", queryId);
        auto abortResult = YqlPlugin_->Abort(queryId);

        if (abortResult.YsonError) {
            *response->mutable_error() = *abortResult.YsonError;
        }

        context->AnnotateResponse()
            .With("Error", abortResult.YsonError);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NYqlPlugin::NProto, GetQueryProgress)
    {
        auto queryId = FromProto<TQueryId>(request->query_id());

        context->AnnotateRequest()
            .With("QueryId", queryId);
        auto queryProgress = YqlPlugin_->GetProgress(queryId);

        auto yqlResponse = ToYqlResponse(queryProgress);

        response->mutable_response()->Swap(&yqlResponse);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NYqlPlugin::NProto, GetDeclaredParametersInfo)
    {
        auto queryId = FromProto<TQueryId>(request->query_id());
        context->AnnotateRequest()
            .With("QueryId", queryId);

        auto result = YqlPlugin_->GetDeclaredParametersInfo(
            queryId,
            request->user(),
            request->query_identity_token(),
            request->query_text(),
            TYsonString(request->settings()),
            TYsonString(request->credentials()));

        if (result.YsonParameters) {
            response->set_yson_parameters(*result.YsonParameters);
        }

        context->AnnotateResponse()
            .With("Parameters", result.YsonParameters);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NYqlPlugin::NProto, RegisterQuery)
    {
        auto queryId = FromProto<TQueryId>(request->query_id());
        context->AnnotateRequest()
            .With("QueryId", queryId);

        YqlPlugin_->RegisterQuery(queryId, TYsonString(request->settings()));

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NYqlPlugin::NProto, UnregisterQuery)
    {
        auto queryId = FromProto<TQueryId>(request->query_id());
        context->AnnotateRequest()
            .With("QueryId", queryId);

        YqlPlugin_->UnregisterQuery(queryId);

        context->Reply();
    }

private:
    const std::unique_ptr<IYqlPlugin> YqlPlugin_;
    const TActionQueuePtr QueryActionQueue_;

    std::vector<TQueryFile> ExtractFiles(const google::protobuf::RepeatedPtrField<TYqlQueryFile>& protoFiles)
    {
        std::vector<TQueryFile> files(protoFiles.size());
        for (const auto& file : protoFiles) {
            files.push_back(NYqlPlugin::TQueryFile{
              .Name = file.name(),
              .Content = file.content(),
              .Type = static_cast<EQueryFileContentType>(file.type()),
            });
        }
        return files;
    }

    TYqlResponse ToYqlResponse(const TQueryResult& queryResult)
    {
        TYqlResponse yqlResponse;

        SetYqlResponseFieldIfValuePresent(yqlResponse, queryResult.Progress, &TYqlResponse::mutable_progress);
        SetYqlResponseFieldIfValuePresent(yqlResponse, queryResult.Plan, &TYqlResponse::mutable_plan);
        SetYqlResponseFieldIfValuePresent(yqlResponse, queryResult.Statistics, &TYqlResponse::mutable_statistics);
        SetYqlResponseFieldIfValuePresent(yqlResponse, queryResult.TaskInfo, &TYqlResponse::mutable_task_info);
        SetYqlResponseFieldIfValuePresent(yqlResponse, queryResult.YsonError, &TYqlResponse::mutable_error);
        SetYqlResponseFieldIfValuePresent(yqlResponse, queryResult.YsonResult, &TYqlResponse::mutable_result);
        SetYqlResponseFieldIfValuePresent(yqlResponse, queryResult.Ast, &TYqlResponse::mutable_ast);

        return yqlResponse;
    }

    void SetYqlResponseFieldIfValuePresent(TYqlResponse& yqlResponse, std::optional<TString> value, TString* (TYqlResponse::*mutableProtoFieldAccessor)())
    {
        if (!value) {
            return;
        }

        *((&yqlResponse)->*mutableProtoFieldAccessor)() = *value;
    }
};

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateYqlPluginService(
    IInvokerPtr controlInvoker,
    std::unique_ptr<IYqlPlugin> yqlAgent)
{
    return New<TYqlPluginService>(std::move(controlInvoker), std::move(yqlAgent));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin::NProcess
