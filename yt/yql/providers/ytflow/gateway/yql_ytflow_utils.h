#pragma once

#include <library/cpp/yson/node/node.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_type_annotation.h>

#include <yt/yt/client/ypath/rich.h>
#include <yt/yt/core/ytree/public.h>

#include <util/generic/string.h>

namespace NYql {

struct TYtflowSettings;

} // namespace NYql


namespace NYql::NYtflow {

class TConfigClusters;

} // namespace NYql::NYtflow


namespace NYql::NYtflow::NPrivate {

void VisitExprCurrentEpoch(
    const TExprNode::TPtr& root, const TExprVisitPtrFunc& func);

TString CanonizeYtPath(
    TString path,
    const TYtflowSettings& config);

NYT::NYPath::TRichYPath CanonizeYtRichPath(
    TString path,
    const TYtflowSettings& config);

TString GetCanonicalPipelinePath(const TYtflowSettings& config);

TString ResolvePipelineClusterName(
    const TYtflowSettings& config,
    const TConfigClusters& configClusters);

NYT::NYPath::TRichYPath MakeYtConsumerRichPath(
    const TYtflowSettings& config,
    const TConfigClusters& configClusters);

TString GetAuth(
    TString cluster,
    const TYtflowSettings& config,
    const TConfigClusters& configClusters);

TString MakeOperationTitle(const TYqlOperationOptions& operationOptions);

NYT::TNode MakeOperationDescription(
    const TYqlOperationOptions& operationOptions,
    const TYtflowSettings& config,
    const TConfigClusters& configClusters);

bool DoesOperationDescriptionMatchPipeline(
    const NYT::NYTree::IMapNodePtr& description,
    const TYtflowSettings& config,
    const TConfigClusters& configClusters);

} // namespace NYql::NYtflow::NPrivate
