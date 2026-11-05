#include "yql_ytflow_provider_impl.h"
#include "yql_ytflow_constants.h"
#include "yql_ytflow_utils.h"

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/providers/common/transform/yql_optimize.h>
#include <yql/essentials/utils/log/log_component.h>

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>
#include <yt/yql/providers/ytflow/integration/interface/yql_ytflow_integration.h>
#include <yt/yql/providers/ytflow/integration/interface/yql_ytflow_optimization.h>
#include <yt/yql/providers/ytflow/integration/proto/yt.pb.h>

#include <google/protobuf/any.pb.h>

#include <utility>


namespace NYql {

using namespace NNodes;


class TYtflowLogicalOptProposalTransformer: public TOptimizeTransformerBase {
public:
    TYtflowLogicalOptProposalTransformer(TYtflowState::TPtr state)
        : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderYtflow, {})
        , State_(std::move(state))
    {
#define HNDL(name) "LogicalOptimizer-"#name, Hndl(&TYtflowLogicalOptProposalTransformer::name)
        AddHandler(0, &TCoExtractMembers::Match, HNDL(ExtractMembersOverReadWrap));
        AddHandler(0, &TCoUnordered::Match, HNDL(UnorderedOverReadWrap));
        AddHandler(0, &TCoSync::Match, HNDL(SyncOverPublish));
        AddHandler(0, &TCoFilterNullMembers::Match, HNDL(FilterNullMembers<TCoFilterNullMembers>));
        AddHandler(0, &TCoSkipNullMembers::Match, HNDL(FilterNullMembers<TCoSkipNullMembers>));
        AddHandler(1, &TYtflowReadWrap::Match, HNDL(ExtractMembersOverReadWrapMultiUsage));
        AddHandler(1, &TYtflowMap::Match, HNDL(MapOverHoppingAggregate));
        AddHandler(1, &TYtflowTransformMap::Match, HNDL(TransformCombineMapOverSourceMap));
        AddHandler(1, &TCoExtractMembers::Match, HNDL(ExtractMembersOverOutput));
        AddHandler(1, &TCoNth::Match, HNDL(NthOverOutput));
        AddHandler(1, &TYtflowPersistentSink::Match, HNDL(PersistentSinkWithSortedYtSink));
#undef HNDL

        SetGlobal(1);
    }

private:
    TMaybeNode<TExprBase> TransformCombineMapOverSourceMap(
        TExprBase node, TExprContext& ctx, const TGetParents& getParents
    ) {
        auto outerTransformMap = node.Cast<TYtflowTransformMap>();
        if (outerTransformMap.Sources().Size() != 1) {
            return node;
        }

        if (outerTransformMap.GroupByColumns().Size() != 1 ||
            outerTransformMap.GroupByColumns().Item(0).Value() != YTFLOW_INPUT_MESSAGE_ID_FIELD
        ) {
            return node;
        }

        auto maybeOutput = outerTransformMap.Sources().Item(0).Maybe<TYtflowOutput>();
        if (!maybeOutput || maybeOutput.Cast().OutputIndex().Value() != "0") {
            return node;
        }

        auto output = maybeOutput.Cast();

        auto maybeInnerSourceMap = output.Operation().Maybe<TYtflowSourceMap>();
        if (!maybeInnerSourceMap) {
            return node;
        }

        auto innerSourceMap = maybeInnerSourceMap.Cast();
        if (innerSourceMap.Sources().Size() != 1 ||
            !innerSourceMap.Sources().Item(0).Maybe<TYtflowPersistentSource>()
        ) {
            return node;
        }

        const auto& parents = *getParents();
        const auto consumers = GetConsumersUnordered(innerSourceMap, parents);
        if (!consumers || consumers->size() != 1) {
            return node;
        }

        if (parents.at(innerSourceMap.Raw()).size() != 1 ||
            parents.at(output.Raw()).size() != 1
        ) {
            return node;
        }

        const auto& [consumer, consumerOutput] = consumers->front();
        if (consumerOutput.OutputIndex().Value() != "0") {
            return node;
        }

        auto innerType = innerSourceMap.Lambda().Ref().GetTypeAnn();
        if (!innerType || innerType->GetKind() != ETypeAnnotationKind::Stream) {
            return node;
        }

        auto innerItemType = innerType->Cast<TStreamExprType>()->GetItemType();
        if (innerItemType->GetKind() != ETypeAnnotationKind::Struct) {
            return node;
        }

        if (!HasSetting(innerSourceMap.Settings().Ref(), INJECT_INPUT_MESSAGE_ID_SETTING)) {
            return node;
        }

        for (const auto& settings : {innerSourceMap.Settings(), outerTransformMap.Settings()}) {
            auto filteredSettings = RemoveSettings(
                settings.Ref(),
                {INJECT_INPUT_MESSAGE_ID_SETTING, HAS_PERSISTENT_SINK_SETTING},
                ctx);

            if (!filteredSettings->Children().empty()) {
                return node;
            }
        }

        return Build<TYtflowTransformSourceMap>(ctx, node.Pos())
            .World<TCoSync>()
                .Add({outerTransformMap.World(), innerSourceMap.World()})
                .Build()
            .Sources(innerSourceMap.Sources())
            .Settings(outerTransformMap.Settings())
            .Lambda<TCoLambda>()
                .Args({"stream"})
                .Body<TExprApplier>()
                    .Apply(outerTransformMap.Lambda())
                    .With<TExprApplier>(0)
                        .Apply(innerSourceMap.Lambda())
                        .With(0, "stream")
                        .Build()
                    .Build()
                .Build()
            .Done();
    }

    TMaybeNode<TExprBase> ExtractMembersOverReadWrap(
        TExprBase node, TExprContext& ctx, const TGetParents& getParents
    ) {
        auto extractMembers = node.Cast<TCoExtractMembers>();
        if (auto maybeReadWrap = extractMembers.Input().Maybe<TYtflowReadWrap>()) {
            auto readWrap = maybeReadWrap.Cast();
            if (getParents()->at(readWrap.Raw()).size() != 1) {
                return node;
            }

            auto input = readWrap.Input();

            if (auto ytflowOptimization = GetYtflowOptimization(input.Ref(), *State_->Types)) {
                auto newReadNode = ytflowOptimization->ApplyExtractMembers(
                    input.Ptr(), extractMembers.Members().Ptr(), ctx
                );
                if (!newReadNode) {
                    return {};
                }

                if (newReadNode != input.Ptr()) {
                    return Build<TYtflowReadWrap>(ctx, node.Pos())
                        .InitFrom(readWrap)
                        .Input(std::move(newReadNode))
                        .Done();
                }
            }
        }

        return node;
    }

    TMaybeNode<TExprBase> ExtractMembersOverReadWrapMultiUsage(
        TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents
    ) {
        auto readWrap = node.Cast<TYtflowReadWrap>();
        auto providerRead = readWrap.Input();
        if (auto ytflowOptimization = GetYtflowOptimization(providerRead.Ref(), *State_->Types)) {
            TNodeOnNodeOwnedMap toOptimize;
            TExprNode::TPtr result;
            bool error = false;
            OptimizeSubsetFieldsForNodeWithMultiUsage(
                node.Ptr(),
                *getParents(),
                toOptimize,
                ctx,
                [&](const TExprNode::TPtr& input, const TExprNode::TPtr& members,
                    const TParentsMap&, TExprContext& ctx) -> TExprNode::TPtr {
                    auto newReadNode = ytflowOptimization->ApplyExtractMembers(
                        providerRead.Ptr(), members, ctx);
                    if (!newReadNode) {
                        error = true;
                        return {};
                    }

                    if (newReadNode != providerRead.Ptr()) {
                        result = ctx.ChangeChild(
                            readWrap.Ref(), TYtflowReadWrap::idx_Input, std::move(newReadNode));
                        return result;
                    }

                    return input;
                });

            if (error) {
                return {};
            }

            if (!toOptimize.empty()) {
                for (auto& [source, destination] : toOptimize) {
                    optCtx.RemapNode(*source, destination);
                }

                return TExprBase(result);
            }
        }

        return node;
    }

    TMaybeNode<TExprBase> UnorderedOverReadWrap(TExprBase node, TExprContext& ctx) {
        auto unordered = node.Cast<TCoUnordered>();
        // TODO(artemmashin): find out why Unordered remains without this if statement
        if (unordered.Input().Maybe<TYtflowOutput>()) {
            return unordered.Input();
        }

        if (auto maybeReadWrap = unordered.Input().Maybe<TYtflowReadWrap>()) {
            auto input = maybeReadWrap.Cast().Input();

            // TODO(artemmashin): remove this after contrib sync
            auto providerName = GetYtflowIntegrationWithProviderName(input.Ref(), *State_->Types).first;
            if (providerName == PqProviderName) {
                return maybeReadWrap.Cast();
            }

            if (auto ytflowOptimization = GetYtflowOptimization(input.Ref(), *State_->Types)) {
                auto newReadNode = ytflowOptimization->ApplyUnordered(input.Ptr(), ctx);
                if (!newReadNode) {
                    return {};
                }

                if (newReadNode != input.Ptr()) {
                    return Build<TYtflowReadWrap>(ctx, node.Pos())
                        .InitFrom(maybeReadWrap.Cast())
                        .Input(std::move(newReadNode))
                        .Done();
                }
            }
        }

        return node;
    }

    TMaybeNode<TExprBase> SyncOverPublish(TExprBase node, TExprContext& ctx) {
        auto sync = node.Cast<TCoSync>();

        TVector<TExprBase> childrenWithPublishStripped;
        for (const auto& arg: sync.Args()) {
            if (auto maybePublish = TMaybeNode<TYtflowPublish>(arg.Get())) {
                childrenWithPublishStripped.push_back(maybePublish.Cast().World());
            }
        }

        if (childrenWithPublishStripped.size() != sync.ArgCount()) {
            return node;
        }

        return Build<TYtflowPublish>(ctx, node.Pos())
            .World<TCoSync>()
                .Add(std::move(childrenWithPublishStripped))
                .Build()
            .Settings()
                .Build()
            .Done();
    }

    TMaybeNode<TExprBase> MapOverHoppingAggregate(
        TExprBase node, TExprContext& ctx, const TGetParents& getParents
    ) {
        // TODO(ngc224): generalize code and move it into physical finalizing stage
        auto outerMap = node.Cast<TYtflowMap>();

        if (outerMap.Sources().Size() != 1) {
            return node;
        }

        if (!IsSingleOutput(outerMap)) {
            return node;
        }

        auto maybeInnerHoppingAggregate = outerMap.Sources().Item(0)
            .Maybe<TYtflowOutput>().Operation()
            .Maybe<TYtflowHoppingAggregate>();

        if (!maybeInnerHoppingAggregate) {
            return node;
        }

        auto innerHoppingAggregate = maybeInnerHoppingAggregate.Cast();

        const auto* parents = getParents();
        if (parents->at(innerHoppingAggregate.Raw()).size() != 1) {
            return node;
        }

        const auto consumers = GetConsumersUnordered(innerHoppingAggregate, *parents);
        if (!consumers || consumers->size() != 1) {
            return node;
        }

        auto postprocessHelperLambda = Build<TCoLambda>(ctx, node.Pos())
            .Args({"applyResult"})
            .Body<TExprList>()
                .Add<TExprApplier>()
                    .Apply(outerMap.Lambda())
                        .With<TCoNth>(0)
                            .Tuple("applyResult")
                            .Index()
                                .Value(0)
                                .Build()
                            .Build()
                        .Build()
                .Add<TCoNth>()
                    .Tuple("applyResult")
                    .Index()
                        .Value(1)
                        .Build()
                    .Build()
                .Add<TCoNth>()
                    .Tuple("applyResult")
                    .Index()
                        .Value(2)
                        .Build()
                    .Build()
                .Build()
            .Done();

        return Build<TYtflowHoppingAggregate>(ctx, node.Pos())
            .InitFrom(innerHoppingAggregate)
            .World<TCoSync>()
                .Add({outerMap.World(), innerHoppingAggregate.World()})
                .Build()
            .Sources(innerHoppingAggregate.Sources())
            .Settings()
                .Add(MergeSettings(
                        innerHoppingAggregate.Settings().Ref(),
                        outerMap.Settings().Ref(),
                        ctx))
                .Build()
            .PostprocessLambda<TCoLambda>()
                .Args({"key", "savedState", "maxHopStartTime"})
                .Body<TExprApplier>()
                    .Apply(postprocessHelperLambda)
                        .With<TExprApplier>(0)
                            .Apply(TCoLambda(innerHoppingAggregate.PostprocessLambda()))
                                .With(0, "key")
                                .With(1, "savedState")
                                .With(2, "maxHopStartTime")
                                .Build()
                        .Build()
                .Build()
            .Done().Ptr();
    }

    TMaybeNode<TExprBase> PersistentSinkWithSortedYtSink(TExprBase node, TExprContext& ctx) {
        auto persistentSink = node.Cast<TYtflowPersistentSink>();

        ::google::protobuf::Any settings;
        if (!TryGetYtSinkSettings(persistentSink.Ref(), ctx, *State_->Types, settings) ||
            !settings.Is<NYtflow::NProto::TYtSortedTableSinkMessage>())
        {
            return node;
        }

        NYtflow::NProto::TYtSortedTableSinkMessage sinkSettings;
        settings.UnpackTo(&sinkSettings);

        TVector<TString> keyColumns(
            sinkSettings.GetKeyColumns().begin(),
            sinkSettings.GetKeyColumns().end());

        if (keyColumns.empty()) {
            return node;
        }

        if (auto maybeTransformMap = persistentSink.Output().Operation().Maybe<TYtflowTransformMap>()) {
            auto transformMap = maybeTransformMap.Cast();
            TVector<TString> groupByColumns;
            for (const auto& column : transformMap.GroupByColumns()) {
                groupByColumns.push_back(column.StringValue());
            }

            if (groupByColumns == keyColumns && IsIdentityLambda(transformMap.Lambda().Ref())) {
                return node;
            }
        }

        TSyncMap syncList;
        auto source = BuildOperationSource(
            persistentSink.Output().Ptr(), syncList, ctx, *State_->Types);
        auto transformMap = Build<TYtflowTransformMap>(ctx, node.Pos())
            .World(ApplySyncListToWorld(ctx.NewWorld(node.Pos()), syncList, ctx))
            .Sources()
                .Add(std::move(source))
                .Build()
            .Settings()
                .Build()
            .Lambda()
                .Args({"stream"})
                .Body("stream")
                .Build()
            .GroupByColumns(MakeAtomList(node.Pos(), keyColumns, ctx))
            .Done();

        return Build<TYtflowPersistentSink>(ctx, node.Pos())
            .InitFrom(persistentSink)
            .World<TCoSync>()
                .Add({transformMap.World(), persistentSink.Write()})
                .Build()
            .Output<TYtflowOutput>()
                .Operation(std::move(transformMap))
                .OutputIndex()
                    .Value(0)
                    .Build()
                .Build()
            .Done();
    }

    template <typename TCallable>
    TMaybeNode<TExprBase> FilterNullMembers(TExprBase node, TExprContext& ctx) const {
        auto filterNullMembers = node.Cast<TCallable>();
        if (!IsYtflowProviderInput(filterNullMembers.Input().Ref())) {
            return node;
        }

        YQL_ENSURE(filterNullMembers.Ptr()->GetTypeAnn()->GetKind() == ETypeAnnotationKind::List);

        return Build<TCoOrderedFlatMap>(ctx, filterNullMembers.Pos())
            .Input(filterNullMembers.Input())
            .Lambda()
                .Args({"item"})
                .template Body<TCallable>()
                    .template Input<TCoJust>()
                        .Input("item")
                        .Build()
                    .Members(filterNullMembers.Members())
                    .Build()
                .Build()
            .Done();
    }

    TMaybeNode<TExprBase> ExtractMembersOverOutput(TExprBase node, TExprContext& ctx) {
        auto extractMembers = node.Cast<TCoExtractMembers>();

        auto maybeOutput = extractMembers.Input().Maybe<TYtflowOutput>();
        if (!maybeOutput) {
            return node;
        }

        auto output = maybeOutput.Cast();

        TSyncMap syncList;
        auto source = BuildOperationSource(
            output.Ptr(), syncList, ctx, *State_->Types);

        auto extractMembersMap = Build<TYtflowMap>(ctx, node.Pos())
            .World(ApplySyncListToWorld(ctx.NewWorld(node.Pos()), syncList, ctx))
            .Sources()
                .Add(std::move(source))
                .Build()
            .Settings()
                .Build()
            .Lambda()
                .Args({"stream"})
                .Body<TCoExtractMembers>()
                    .InitFrom(extractMembers)
                    .Input("stream")
                    .Build()
                .Build()
            .Done();

        return Build<TYtflowOutput>(ctx, node.Pos())
            .Operation(std::move(extractMembersMap))
            .OutputIndex()
                .Value(0)
                .Build()
            .Done().Ptr();
    }

    TMaybeNode<TExprBase> NthOverOutput(TExprBase node, TExprContext& ctx) {
        auto nth = node.Cast<TCoNth>();
        auto maybeInput = nth
            .Tuple().Maybe<TCoDemux>()
            .Input().Maybe<TCoRight>().Input()
            .Maybe<TYtflowMapBase>();

        if (!maybeInput) {
            return node;
        }

        return Build<TYtflowOutput>(ctx, node.Pos())
            .Operation(maybeInput.Cast())
            .OutputIndex(nth.Index())
            .Done();
    }

private:
    TYtflowState::TPtr State_;
};


THolder<IGraphTransformer> CreateYtflowLogicalOptProposalTransformer(TYtflowState::TPtr state) {
    return MakeHolder<TYtflowLogicalOptProposalTransformer>(std::move(state));
}

} // namespace NYql
