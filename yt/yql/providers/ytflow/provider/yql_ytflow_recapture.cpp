#include "yql_ytflow_provider_impl.h"
#include "yql_ytflow_state.h"
#include "yql_ytflow_utils.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/utils/log/log.h>

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>
#include <yt/yql/providers/ytflow/integration/interface/yql_ytflow_integration.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>

#include <deque>


namespace NYql {

using namespace NNodes;


class TYtflowRecaptureOptProposalTransformer: public TSyncTransformerBase {
public:
    TYtflowRecaptureOptProposalTransformer(TYtflowState::TPtr state)
        : State_(std::move(state))
    {
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;
        if (ctx.Step.IsDone(TExprStep::Recapture)) {
            return TStatus::Ok;
        }

        if (State_->Types->EngineType != EEngineType::Ytflow) {
            return TStatus::Ok;
        }

        if (auto status = RecaptureIO(output, output, ctx); status != TStatus::Ok) {
            return status;
        }

        if (auto status = ReorderGraph(output, output, ctx); status != TStatus::Ok) {
            return status;
        }

        return TStatus::Ok;
    }

    TStatus RecaptureIO(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
        output = input;

        auto& typesCtx = *State_->Types;
        auto settings = TOptimizeExprSettings(&typesCtx);
        settings.VisitChecker = [&](const TExprNode& node) {
            return &node == input.Get() || !(
                TYtflowReadWrap::Match(&node) ||
                TYtflowWriteWrap::Match(&node)
            );
        };

        auto status = OptimizeExpr(
            output, output,
            [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr
        {
            if (auto maybeRead = TMaybeNode<TCoRight>(node).Input()) {
                if (maybeRead.Raw()->ChildrenSize() > 1
                    && TCoDataSource::Match(maybeRead.Raw()->Child(1)))
                {
                    auto dataSourceName = maybeRead.Raw()->Child(1)->Child(0)->Content();
                    auto dataSource = typesCtx.DataSourceMap.FindPtr(dataSourceName);
                    YQL_ENSURE(dataSource);

                    if (auto ytflowIntegration = (*dataSource)->GetYtflowIntegration()) {
                        if (auto canRead = ytflowIntegration->CanRead(maybeRead.Ref(), ctx)) {
                            if (!canRead.GetRef()) {
                                return nullptr;
                            }

                            auto newRead = ytflowIntegration->WrapRead(maybeRead.Cast().Ptr(), ctx);
                            YQL_ENSURE(TMaybeNode<TYtflowReadWrap>(newRead));

                            return newRead;
                        }
                    }
                }
            } else if (node->GetTypeAnn()->GetKind() == ETypeAnnotationKind::World
                && !TCoCommit::Match(node.Get())
                && node->ChildrenSize() > 1
                && TCoDataSink::Match(node->Child(1)))
            {
                auto dataSinkName = node->Child(1)->Child(0)->Content();
                auto dataSink = typesCtx.DataSinkMap.FindPtr(dataSinkName);
                YQL_ENSURE(dataSink);

                if (auto ytflowIntegration = (*dataSink)->GetYtflowIntegration()) {
                    if (auto canWrite = ytflowIntegration->CanWrite(*node, ctx)) {
                        if (!canWrite.GetRef()) {
                            return nullptr;
                        }

                        auto writeContentWithoutSystemMembers = Build<TCoRemoveSystemMembers>(ctx, node->Pos())
                            .Input(ytflowIntegration->GetWriteContent(*node, ctx))
                            .Done().Ptr();

                        auto newWrite = ytflowIntegration->UpdateWriteContent(
                            node, writeContentWithoutSystemMembers, ctx);

                        auto wrappedWrite = ytflowIntegration->WrapWrite(newWrite, ctx);
                        YQL_ENSURE(TMaybeNode<TYtflowWriteWrap>(wrappedWrite));

                        return Build<TYtflowPublish>(ctx, node->Pos())
                            .World(std::move(wrappedWrite))
                            .Settings()
                                .Build()
                            .Done().Ptr();
                    }
                }
            }

            return node;
        }, ctx, settings);

        if (output != input) {
            YQL_CLOG(INFO, ProviderYtflow) << "Recapture-RecaptureIO";
        }

        return status;
    }

    TStatus ReorderGraph(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
        output = input;

        TVector<TExprNode::TPtr> publishNodes;

        VisitExprByFirst(output, [&publishNodes](const TExprNode::TPtr& node) {
            if (TMaybeNode<TYtflowPublish>(node)) {
                publishNodes.push_back(node);
            }

            return true;
        });

        // remaps for root publish nodes:
        // top level publish -> sync over top level publish + all nested publish nodes
        TNodeOnNodeOwnedMap topLevelRemaps;
        ui32 newPublishNodeCount = 0;

        TNodeSet visitedPublishNodes;
        auto& typesCtx = *State_->Types;

        for (const auto& publishNode : publishNodes) {
            if (visitedPublishNodes.contains(publishNode.Get())) {
                continue;
            }

            std::deque<TExprNode::TPtr> nestedPublishNodes;
            VisitExprByFirst(publishNode, [&nestedPublishNodes](
                const TExprNode::TPtr& node
            ) {
                if (TMaybeNode<TCoCommit>(node)) {
                    return false;
                }

                if (TMaybeNode<TYtflowPublish>(node)) {
                    nestedPublishNodes.push_front(node);
                }

                return true;
            });

            // no actual nested publishes as self is included in list
            if (nestedPublishNodes.size() == 1) {
                visitedPublishNodes.emplace(publishNode.Get());
                continue;
            }

            // remaps for nested publish nodes:
            // publish node -> world from corresponding provider write
            // (provider write is cleaned up from other publish nodes)
            TNodeOnNodeOwnedMap remaps;
            TVector<TExprNode::TPtr> newNestedPublishNodes;

            auto nestedOptimizeSettings = TOptimizeExprSettings(&typesCtx);
            nestedOptimizeSettings.VisitChanges = true;

            for (const auto& nestedPublishNode : nestedPublishNodes) {
                if (visitedPublishNodes.contains(nestedPublishNode.Get())) {
                    continue;
                }

                auto nestedPublish = TYtflowPublish(nestedPublishNode);

                auto maybeWriteWrap = nestedPublish.World().Maybe<TYtflowWriteWrap>();
                YQL_ENSURE(maybeWriteWrap);

                auto providerWrite = maybeWriteWrap.Cast().Input().Ptr();
                auto* ytflowIntegration = GetYtflowIntegration(*providerWrite, typesCtx);
                YQL_ENSURE(ytflowIntegration);

                auto writeWorld = ytflowIntegration->GetWriteWorld(*providerWrite, ctx);
                TExprNode::TPtr newWriteWorld;

                auto remapStatus = RemapExpr(
                    writeWorld, newWriteWorld, remaps, ctx, nestedOptimizeSettings);

                if (remapStatus != TStatus::Ok) {
                    YQL_ENSURE(remapStatus == TStatus::Error);
                    return remapStatus;
                }

                remaps.emplace(nestedPublishNode.Get(), newWriteWorld);

                auto newProviderWrite = ytflowIntegration->UpdateWriteWorld(
                    providerWrite, newWriteWorld, ctx);

                auto newWriteWrap = ctx.ChangeChild(
                    maybeWriteWrap.Cast().Ref(),
                    TYtflowWriteWrap::idx_Input,
                    std::move(newProviderWrite));

                auto newNestedPublishNode = ctx.ChangeChild(
                    *nestedPublishNode,
                    TYtflowPublish::idx_World,
                    std::move(newWriteWrap));

                newNestedPublishNodes.push_back(newNestedPublishNode);

                visitedPublishNodes.emplace(nestedPublishNode.Get());
            }

            newPublishNodeCount += newNestedPublishNodes.size();
            topLevelRemaps[publishNode.Get()] = Build<TCoSync>(ctx, publishNode->Pos())
                .Add(std::move(newNestedPublishNodes))
                .Done().Ptr();
        }

        if (topLevelRemaps.empty()) {
            return TStatus::Ok;
        }

        YQL_ENSURE(newPublishNodeCount == publishNodes.size());

        auto optimizeSettings = TOptimizeExprSettings(&typesCtx);

        auto status = RemapExpr(input, output, topLevelRemaps, ctx, optimizeSettings);

        if (output != input) {
            YQL_CLOG(INFO, ProviderYtflow) << "Recapture-ReorderGraph";
        }

        return status;
    }

    void Rewind() final {
    }

private:
    TYtflowState::TPtr State_;
};

THolder<IGraphTransformer> CreateYtflowRecaptureOptProposalTransformer(TYtflowState::TPtr state) {
    return MakeHolder<TYtflowRecaptureOptProposalTransformer>(std::move(state));
}

} // NYql
