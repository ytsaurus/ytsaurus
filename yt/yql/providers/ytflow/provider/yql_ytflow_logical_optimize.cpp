#include "yql_ytflow_provider_impl.h"
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

#include <library/cpp/iterator/enumerate.h>
#include <library/cpp/yt/string/format.h>

#include <util/generic/adaptor.h>
#include <util/generic/algorithm.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/string/cast.h>

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
        AddHandler(0, &TYtflowWriteWrap::Match, HNDL(WriteWrapOverSort));
        AddHandler(0, &TCoFilterNullMembers::Match, HNDL(FilterNullMembers<TCoFilterNullMembers>));
        AddHandler(0, &TCoSkipNullMembers::Match, HNDL(FilterNullMembers<TCoSkipNullMembers>));
        AddHandler(1, &TYtflowReadWrap::Match, HNDL(ExtractMembersOverReadWrapMultiUsage));
        AddHandler(1, &TYtflowMap::Match, HNDL(MapOverHoppingAggregate));
        AddHandler(1, &TCoExtractMembers::Match, HNDL(ExtractMembersOverOutput));
        AddHandler(1, &TCoNth::Match, HNDL(NthOverOutput));
        AddHandler(1, &TYtflowOpBase::Match, HNDL(OpBaseWithSortedYtPersistentSinks));
#undef HNDL

        SetGlobal(1);
    }

private:
    struct TSortedYtSinkInfo {
        TExprNode::TPtr Sink;
        TVector<TString> KeyColumns;
        ui32 OutputIndex;
    };

private:
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

    TMaybeNode<TExprBase> WriteWrapOverSort(TExprBase node, TExprContext& ctx) {
        auto writeWrap = node.Cast<TYtflowWriteWrap>();
        auto providerWrite = writeWrap.Input();

        auto* ytflowIntegration = GetYtflowIntegration(providerWrite.Ref(), *State_->Types);
        YQL_ENSURE(ytflowIntegration, "Unknown provider write: " << providerWrite.Ref().Content());

        auto content = ytflowIntegration->GetWriteContent(providerWrite.Ref(), ctx);

        auto maybeSort = TMaybeNode<TCoSort>(content);
        if (!maybeSort) {
            return node;
        }

        auto sort = maybeSort.Cast();

        auto* ytflowOptimization = GetYtflowOptimization(providerWrite.Ref(), *State_->Types);
        YQL_ENSURE(ytflowOptimization, "Unknown provider write: " << providerWrite.Ref().Content());

        auto writeWithoutSort = ytflowIntegration->UpdateWriteContent(
            providerWrite.Ptr(), sort.Input().Ptr(), ctx);

        auto newProviderWrite = ytflowOptimization->ApplySort(
            writeWithoutSort, sort.Ptr(), ctx);

        if (!newProviderWrite) {
            return {};
        }

        return Build<TYtflowWriteWrap>(ctx, node.Pos())
            .InitFrom(writeWrap)
            .Input(std::move(newProviderWrite))
            .Done();
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

        if (outerMap.Sinks().Size() != 1) {
            return node;
        }

        auto maybeInnerHoppingAggregate = outerMap.Sources().Item(0)
            .Maybe<TYtflowOutput>().Operation()
            .Maybe<TYtflowHoppingAggregate>();

        if (!maybeInnerHoppingAggregate) {
            return node;
        }

        auto innerHoppingAggregate = maybeInnerHoppingAggregate.Cast();

        if (innerHoppingAggregate.Sinks().Size() != 1) {
            return node;
        }

        const auto* parents = getParents();
        if (parents->at(innerHoppingAggregate.Raw()).size() != 1) {
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
            .Sinks(outerMap.Sinks())
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

    TMaybeNode<TExprBase> OpBaseWithSortedYtPersistentSinks(
        TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents
    ) {
        auto opBase = node.Cast<TYtflowOpBase>();

        TVector<TSortedYtSinkInfo> sortedYtSinks;
        TVector<TExprNode::TPtr> resultSinks;
        for (auto [index, sink] : Enumerate(opBase.Sinks())) {
            NYtflow::NProto::TQYTSinkMessage sinkSettings;
            if (!TryGetYtSinkSettings(sink.Ref(), ctx, *State_->Types, sinkSettings)) {
                resultSinks.push_back(sink.Ptr());
                continue;
            }

            TVector<TString> keyColumns(
                sinkSettings.GetKeyColumns().begin(), sinkSettings.GetKeyColumns().end());

            if (keyColumns.empty()) {
                resultSinks.push_back(sink.Ptr());
                continue;
            }

            auto outputIndex = ::FromString<ui32>(sink.Cast<TYtflowSinkBase>().OutputIndex());
            sortedYtSinks.push_back(TSortedYtSinkInfo{
                .Sink = sink.Ptr(),
                .KeyColumns = std::move(keyColumns),
                .OutputIndex = outputIndex,
            });

            resultSinks.push_back(Build<TYtflowIntermediateSink>(ctx, opBase.Sinks().Pos())
                .Name()
                    .Value("")
                    .Build()
                .OutputIndex()
                    .Value(outputIndex)
                    .Build()
                .RowType(
                    ExpandType(node.Pos(), GetSeqItemType(*sink.Ref().GetTypeAnn()), ctx))
                .Done().Ptr());
        }

        if (sortedYtSinks.empty()) {
            return node;
        }

        if (auto maybeTransformMap = node.Maybe<TYtflowTransformMap>()) {
            auto transformMap = maybeTransformMap.Cast();

            TVector<TString> groupByColumns;
            for (const auto& column : transformMap.GroupByColumns()) {
                groupByColumns.push_back(column.StringValue());
            }

            const bool groupByAndKeyColumnsAreSame = AllOf(sortedYtSinks,
                [&groupByColumns](const auto& sortedSinkInfo) {
                    return sortedSinkInfo.KeyColumns == groupByColumns;
                });

            if (groupByAndKeyColumnsAreSame && IsIdentityLambda(transformMap.Lambda().Ref())) {
                return node;
            }
        }

        auto newOpBase = ctx.ChangeChild(
            opBase.Ref(),
            TYtflowOpBase::idx_Sinks,
            Build<TExprList>(ctx, opBase.Sinks().Pos())
                .Add(resultSinks)
                .Done().Ptr());

        TVector<TExprBase> transformMapWorlds;
        for (const auto& sortedYtSinkInfo : sortedYtSinks) {
            TSyncMap syncList;
            auto source = BuildOperationSource(Build<TYtflowOutput>(ctx, node.Pos())
                .Operation(newOpBase)
                .OutputIndex()
                    .Value(sortedYtSinkInfo.OutputIndex)
                    .Build()
                .Done().Ptr(),
                syncList, ctx, *State_->Types);

            const auto& sink = sortedYtSinkInfo.Sink;
            TVector<TExprNode::TPtr> transformMapSinks{ctx.ChangeChild(
                    *sink, TYtflowSinkBase::idx_OutputIndex, ctx.NewAtom(sink->Pos(), 0U))};

            auto transformMap = Build<TYtflowTransformMap>(ctx, node.Pos())
                .World(ApplySyncListToWorld(ctx.NewWorld(node.Pos()), syncList, ctx))
                .Sources()
                    .Add(std::move(source))
                    .Build()
                .Sinks()
                    .Add(std::move(transformMapSinks))
                    .Build()
                .Settings()
                    .Build()
                .Lambda()
                    .Args({"stream"})
                    .Body("stream")
                    .Build()
                .GroupByColumns(
                    MakeAtomList(node.Pos(), sortedYtSinkInfo.KeyColumns, ctx))
                .Done();

            transformMapWorlds.push_back(
                Build<TCoLeft>(ctx, node.Pos())
                    .Input(std::move(transformMap))
                    .Done());
        }

        const auto* parents = getParents();
        auto parentsIterator = parents->find(opBase.Raw());
        YQL_ENSURE(parentsIterator != parents->end(),
            "Unknown parent of " << opBase.Ref().Content());

        TVector<TExprBase> leftNodes;
        for (const auto* parent : parentsIterator->second) {
            if (TMaybeNode<TCoLeft>(parent)) {
                leftNodes.push_back(TExprBase(parent));
                continue;
            }

            YQL_ENSURE(TMaybeNode<TYtflowOutput>(parent),
                "Unexpected parent of " << opBase.Ref().Content() << ": " << parent->Content());
        }

        bool allResultSinksAreIntermediate = AllOf(resultSinks,
            [](const auto& sink) {
                return TMaybeNode<TYtflowIntermediateSink>(sink);
            });

        if (!allResultSinksAreIntermediate) {
            for (const auto& left : leftNodes) {
                transformMapWorlds.push_back(Build<TCoLeft>(ctx, left.Pos())
                    .InitFrom(left.Cast<TCoLeft>())
                    .Done());
            }
        }

        auto sync = Build<TCoSync>(ctx, node.Pos())
            .Add(std::move(transformMapWorlds))
            .Done();

        for (const auto& left : leftNodes) {
            optCtx.RemapNode(left.Ref(), sync.Ptr());
        }

        return TExprBase(newOpBase);
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

        auto extractMembersItemType = GetSeqItemType(extractMembers.Ref().GetTypeAnn());
        YQL_ENSURE(extractMembersItemType->GetKind() == ETypeAnnotationKind::Struct,
            "Unexpected " << extractMembers.Ref().Content() << " item type: " << *extractMembersItemType);

        auto extractMembersMap = Build<TYtflowMap>(ctx, node.Pos())
            .World(ApplySyncListToWorld(ctx.NewWorld(node.Pos()), syncList, ctx))
            .Sources()
                .Add(std::move(source))
                .Build()
            .Sinks()
                .Add<TYtflowIntermediateSink>()
                    .Name()
                        .Value("")
                        .Build()
                    .OutputIndex()
                        .Value(0)
                        .Build()
                    .RowType(ExpandType(TPositionHandle{}, *extractMembersItemType, ctx))
                    .Build()
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
