#include "yql_ytflow_prepare_common.h"
#include "yql_ytflow_utils.h"

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>
#include <yt/yql/providers/ytflow/provider/yql_ytflow_utils.h>
#include <yt/yql/providers/ytflow/integration/interface/yql_ytflow_integration.h>


namespace NYql::NYtflow::NPrepare::NPrivate {

using namespace NNodes;


void VisitPersistentSourceSettings(
    const TExprNode::TPtr& root,
    TContext& prepareCtx,
    const TSettingsVisitor& visitor
) {
    auto extractSourceSettings = [&prepareCtx, &visitor](const TExprNode::TPtr& child) {
        auto maybeOp = TMaybeNode<TYtflowOpBase>(child);
        if (!maybeOp) {
            return true;
        }

        for (const auto& sink : maybeOp.Cast().Sources()) {
            auto maybePersistentSource = sink.Maybe<TYtflowPersistentSource>();
            if (!maybePersistentSource) {
                continue;
            }

            auto input = maybePersistentSource.Cast().Input();
            auto providerInput = input.Cast<TYtflowReadWrap>().Input();

            auto* ytflowIntegration = GetYtflowIntegration(
                providerInput.Ref(),
                *prepareCtx.RunOptions.Types());

            YQL_ENSURE(ytflowIntegration);

            ::google::protobuf::Any sourceSettings;
            ytflowIntegration->FillSourceSettings(
                providerInput.Ref(), sourceSettings, prepareCtx.ExprContext);

            visitor(sourceSettings);
        }

        return true;
    };

    ::NYql::NYtflow::NPrivate::VisitExprCurrentEpoch(root, extractSourceSettings);
}

void VisitPersistentSinkSettings(
    const TExprNode::TPtr& root,
    TContext& prepareCtx,
    const TSettingsVisitor& visitor
) {
    auto extractSinkSettings = [&prepareCtx, &visitor](const TExprNode::TPtr& child) {
        auto maybeOp = TMaybeNode<TYtflowOpBase>(child);
        if (!maybeOp) {
            return true;
        }

        for (const auto& sink : maybeOp.Cast().Sinks()) {
            auto maybePersistentSink = sink.Maybe<TYtflowPersistentSink>();
            if (!maybePersistentSink) {
                continue;
            }

            auto input = maybePersistentSink.Cast().Input();
            auto providerInput = input.Cast<TYtflowWriteWrap>().Input();

            auto* ytflowIntegration = GetYtflowIntegration(
                providerInput.Ref(),
                *prepareCtx.RunOptions.Types());

            YQL_ENSURE(ytflowIntegration);

            ::google::protobuf::Any sinkSettings;
            ytflowIntegration->FillSinkSettings(
                providerInput.Ref(), sinkSettings, prepareCtx.ExprContext);

            visitor(sinkSettings);
        }

        return true;
    };

    ::NYql::NYtflow::NPrivate::VisitExprCurrentEpoch(root, extractSinkSettings);
}

} // namespace NYql::NYtflow::NPrepare::NPrivate
