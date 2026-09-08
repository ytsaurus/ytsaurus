#include "yql_ytflow_utils.h"
#include "yql_ytflow_config_clusters.h"

#include <library/cpp/yt/string/format.h>

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/utils/yql_panic.h>

#include <yt/yql/providers/ytflow/provider/yql_ytflow_configuration.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>

#include <utility>


namespace NYql::NYtflow::NPrivate {

using namespace NNodes;

void VisitExprCurrentEpoch(
    const TExprNode::TPtr& root, const TExprVisitPtrFunc& func)
{
    VisitExpr(root, [&](const TExprNode::TPtr& child) {
        return !TMaybeNode<TCoCommit>(child);
    }, func);
}

namespace {

static const TString YT_ROOT_PATH_PREFIX = "//";

TString MakeAbsolutePath(TString path, TString prefix)
{
    if (!path.StartsWith(YT_ROOT_PATH_PREFIX)) {
        if (!prefix) {
            prefix = YT_ROOT_PATH_PREFIX;
        }

        return TString(prefix).append(path);
    }

    return path;
}

} // namespace

TString CanonizeYtPath(
    TString path,
    const TYtflowSettings& config)
{
    return MakeAbsolutePath(
        std::move(path),
        config.PathPrefix.Get().GetOrElse(""));
}

NYT::NYPath::TRichYPath CanonizeYtRichPath(
    TString path,
    const TYtflowSettings& config)
{
    // TRichYPath rejects relative paths, while default consumer and producer
    // paths derived from PipelinePath may be relative.
    if (!path.StartsWith('<')) {
        return NYT::NYPath::TRichYPath(
            CanonizeYtPath(std::move(path), config));
    }

    auto richPath = NYT::NYPath::TRichYPath(std::move(path));
    richPath.SetPath(CanonizeYtPath(richPath.GetPath(), config));
    return richPath;
}

TString GetCanonicalPipelinePath(const TYtflowSettings& config)
{
    return CanonizeYtPath(config.GetPipelinePath(), config);
}

TString ResolvePipelineClusterName(
    const TYtflowSettings& config,
    const TConfigClusters& configClusters)
{
    auto cluster = config.Cluster.Get();
    YQL_ENSURE(cluster, "Pipeline cluster is not set");

    return configClusters.GetRealName(*cluster);
}

NYT::NYPath::TRichYPath MakeYtConsumerRichPath(
    const TYtflowSettings& config,
    const TConfigClusters& configClusters)
{
    auto consumerPath = CanonizeYtRichPath(
        config.GetYtConsumerPath(), config);

    if (auto cluster = consumerPath.GetCluster()) {
        consumerPath.SetCluster(configClusters.GetRealName(TString(*cluster)));
    } else {
        consumerPath.SetCluster(
            ResolvePipelineClusterName(config, configClusters));
    }

    return consumerPath;
}

TString GetAuth(
    TString cluster,
    const TYtflowSettings& config,
    const TConfigClusters& configClusters)
{
    TString token;
    if (auto auth = config.Auth.Get()) {
        token = *auth;
    } else {
        token = configClusters.GetAuth(cluster);
    }

    YQL_ENSURE(token, "No valid ytflow token provided");

    return token;
}

TString MakeOperationTitle(const TYqlOperationOptions& operationOptions)
{
    if (auto operationTitle = operationOptions.Title) {
        return *operationTitle;
    }

    TStringBuilder titleBuilder;
    titleBuilder << "YQL streaming operation (";

    if (operationOptions.QueryName) {
        titleBuilder << *operationOptions.QueryName;
    }

    if (operationOptions.Id) {
        if (operationOptions.QueryName) {
            titleBuilder << ", ";
        }

        titleBuilder << *operationOptions.Id;
    }

    titleBuilder
        << " by " << operationOptions.AuthenticatedUser.GetOrElse("unknown")
        << ')';

    return titleBuilder;
}

NYT::TNode MakeOperationDescription(
    const TYqlOperationOptions& operationOptions,
    const TYtflowSettings& config,
    const TConfigClusters& configClusters)
{
    auto clusterRealName = ResolvePipelineClusterName(config, configClusters);
    auto absolutePipelinePath = GetCanonicalPipelinePath(config);

    NYT::TNode description = NYT::TNode::CreateMap();

    description["yql_runner"] = operationOptions.Runner;

    if (auto id = operationOptions.Id) {
        description["yql_op_id"] = *id;
    }

    description["yql_pipeline_path"] = absolutePipelinePath;
    description["yql_pipeline_cluster"] = clusterRealName;

    auto uiOrigin = config._UIOrigin.Get();

    // TODO(ngc224): move formatting into UI to eliminate knowledge of concrete UI paths
    if (uiOrigin) {
        NYT::TNode& urlNode = description["yql_pipeline_url"];
        urlNode = NYT::Format(
            "%v/%v/flows/graph?path=%v",
            *uiOrigin,
            clusterRealName,
            config.GetPipelinePath());

        urlNode.Attributes()["_type_tag"] = "url";
    }

    // TODO(ngc224): rewrite into pure Url option, for now it's not supported by UI
    if (auto url = operationOptions.Url;
        url && uiOrigin && operationOptions.Id
    ) {
        NYT::TNode& urlNode = description["yql_op_url"];
        urlNode = NYT::Format(
            "%v/%v/queries/%v",
            *uiOrigin,
            *url,
            *operationOptions.Id);

        urlNode.Attributes()["_type_tag"] = "url";
    }

    if (auto title = operationOptions.Title) {
        description["yql_op_title"] = *title;
    }

    if (auto name = operationOptions.QueryName) {
        description["yql_query_name"] = *name;
    }

    if (auto attrsYson = operationOptions.AttrsYson) {
        NYT::TNode userAttributes = NYT::NodeFromYsonString(*attrsYson);
        for (const auto& [key, value] : userAttributes.AsMap()) {
            auto keyBuf = TStringBuf(key);

            // patch common yql-agent attribute to avoid double yql prefix
            if (keyBuf == TStringBuf("yql_version")) {
                keyBuf = TStringBuf("version");
            }

            if (keyBuf != TStringBuf("runner") &&
                keyBuf != TStringBuf("op_id") &&
                keyBuf != TStringBuf("op_url") &&
                keyBuf != TStringBuf("op_title") &&
                keyBuf != TStringBuf("query_name") &&
                keyBuf != TStringBuf("op_code"))
            {
                // do not allow to override specific attrs
                description[TString("yql_") + keyBuf] = value;
            }
        }
    }

    return description;
}

bool DoesOperationDescriptionMatchPipeline(
    const NYT::NYTree::IMapNodePtr& description,
    const TYtflowSettings& config,
    const TConfigClusters& configClusters)
{
    auto providedPipelineCluster = description->FindChildValue<TString>(
        "yql_pipeline_cluster");
    if (providedPipelineCluster != ResolvePipelineClusterName(config, configClusters)) {
        return false;
    }

    auto providedPipelinePath = description->FindChildValue<TString>(
        "yql_pipeline_path");
    if (!providedPipelinePath) {
        return false;
    }

    return CanonizeYtPath(*providedPipelinePath, config) ==
        GetCanonicalPipelinePath(config);
}

} // namespace NYql::NYtflow::NPrivate
