#include "yql_ytflow_provider_impl.h"
#include "yql_ytflow_constants.h"
#include "yql_ytflow_join_utils.h"
#include "yql_ytflow_utils.h"

#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_join.h>
#include <yql/essentials/core/yql_opt_hopping.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/providers/common/transform/yql_optimize.h>
#include <yql/essentials/utils/log/log_component.h>

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>
#include <yt/yql/providers/ytflow/integration/interface/yql_ytflow_integration.h>
#include <yt/yql/providers/ytflow/integration/interface/yql_ytflow_optimization.h>

#include <library/cpp/iterator/enumerate.h>

#include <util/generic/algorithm.h>
#include <util/generic/vector.h>

#include <util/string/cast.h>


namespace NYql {

using namespace NNodes;

namespace {

TExprNode::TPtr BuildLambdaFromSExprFactory(
    TStringBuf factorySExpr,
    TExprNode::TListType dependencies,
    TPositionHandle pos,
    TExprContext& ctx)
{
    auto factoryAst = ParseAst(factorySExpr);
    YQL_ENSURE(factoryAst.IsOk());

    TExprNode::TPtr factory;
    YQL_ENSURE(CompileExpr(*factoryAst.Root, factory, ctx, nullptr, nullptr));

    dependencies.insert(dependencies.begin(), std::move(factory));
    auto factoryApply = ctx.NewCallable(pos, "Apply", std::move(dependencies));

    TExprNode::TPtr lambda;
    ctx.Step.Repeat(TExprStep::ExpandApplyForLambdas);
    YQL_ENSURE(
        ExpandApplyNoRepeat(factoryApply, lambda, ctx) == IGraphTransformer::TStatus::Ok
    );

    YQL_ENSURE(lambda->IsLambda());

    return lambda;
}

} // anonymous namespace

class TYtflowPhysicalOptProposalTransformer: public TOptimizeTransformerBase {
public:
    TYtflowPhysicalOptProposalTransformer(TYtflowState::TPtr state)
        : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderYtflow, {})
        , State_(std::move(state))
    {
#define HNDL(name) "PhysicalOptimizer-"#name, Hndl(&TYtflowPhysicalOptProposalTransformer::name)
        AddHandler(0, &TCoFlatMapBase::Match, HNDL(FlatMapBase));
        AddHandler(0, &TCoExtend::Match, HNDL(Extend));
        AddHandler(0, &TCoEquiJoin::Match, HNDL(EquiJoin));
        AddHandler(0, &TYtflowWriteWrap::Match, HNDL(WriteWrapWithReadWrap));
        AddHandler(0, &TCoAggregate::Match, HNDL(Aggregate));
#undef HNDL
    }

private:
    TMaybeNode<TExprBase> FlatMapBase(TExprBase node, TExprContext& ctx) {
        auto flatMap = node.Cast<TCoFlatMapBase>();
        auto input = flatMap.Input();
        TMaybeNode<TCoUnordered> unordered;
        if (auto maybeUnordered = input.Maybe<TCoUnordered>()) {
            unordered = maybeUnordered;
            input = maybeUnordered.Cast().Input();
        }

        TMaybeNode<TCoExtractMembers> extractMembers;
        TExprBase sourceInput = input;

        if (auto maybeExtractMembers = input.Maybe<TCoExtractMembers>()) {
            if (!maybeExtractMembers.Cast().Input().Maybe<TYtflowReadWrap>()) {
                return node;
            }

            extractMembers = maybeExtractMembers;
            sourceInput = maybeExtractMembers.Cast().Input();
        }

        if (!IsYtflowProviderInput(sourceInput.Ref())) {
            return node;
        }

        auto type = node.Ref().GetTypeAnn();
        if (!type) {
            return node;
        }

        if (!EnsureListType(node.Ref(), ctx)) {
            return node;
        }

        TTypeAnnotationNode::TListType resultTypes;
        auto* itemType = type->Cast<TListExprType>()->GetItemType();
        if (itemType->GetKind() == ETypeAnnotationKind::Variant) {
            auto underlyingType = itemType->Cast<TVariantExprType>()->GetUnderlyingType();
            if (!EnsureTupleType(node.Pos(), *underlyingType, ctx)) {
                return node;
            }

            auto& tupleTypes = underlyingType->Cast<TTupleExprType>()->GetItems();
            for (auto* type : tupleTypes) {
                if (!EnsureStructType(node.Pos(), *type, ctx)) {
                    return node;
                }
            }

            resultTypes = tupleTypes;
        } else if (itemType->GetKind() == ETypeAnnotationKind::Struct) {
            resultTypes.push_back(itemType);
        } else {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()),
                TStringBuilder()
                    << "Expected struct or variant type, but got: " << *itemType));
            return node;
        }

        TSyncMap syncList;
        auto source = BuildOperationSource(sourceInput.Ptr(), syncList, ctx, *State_->Types);

        auto callableName = TMaybeNode<TYtflowPersistentSource>(source)
            ? TYtflowSourceMap::CallableName()
            : TYtflowMap::CallableName();

        TVector<TExprBase> sinks;
        sinks.reserve(resultTypes.size());
        for (auto [index, resultType] : Enumerate(resultTypes)) {
            sinks.push_back(Build<TYtflowIntermediateSink>(ctx, TPositionHandle{})
                .Name()
                    .Value("")
                    .Build()
                .OutputIndex()
                    .Value(index)
                    .Build()
                .RowType(ExpandType(node.Pos(), *resultType, ctx))
                .Done());
        }

        auto streamArg = Build<TCoArgument>(ctx, node.Pos())
            .Name("stream")
            .Done();
        TExprBase computationInput = streamArg;
        if (extractMembers) {
            computationInput = Build<TCoExtractMembers>(ctx, node.Pos())
                .Input(streamArg)
                .Members(extractMembers.Cast().Members())
                .Done();
        }
        if (unordered) {
            computationInput = Build<TCoUnordered>(ctx, node.Pos())
                .Input(computationInput)
                .Done();
        }

        auto lambda = Build<TCoLambda>(ctx, node.Pos())
            .Args({streamArg})
            .Body<TCoFlatMapBase>()
                .CallableName(flatMap.Ref().Content())
                .Input(computationInput)
                .Lambda<TCoLambda>()
                    .Args({"item"})
                    .Body<TExprApplier>()
                        .Apply(flatMap.Lambda())
                            .With(0, "item")
                            .Build()
                    .Build()
                .Build()
            .Done();

        auto map = Build<TYtflowMapBase>(ctx, node.Pos())
            .CallableName(callableName)
            .World(MakeSyncNodeFromSyncList(syncList, node.Pos(), ctx))
            .Sources()
                .Add(std::move(source))
                .Build()
            .Sinks()
                .Add(std::move(sinks))
                .Build()
            .Settings()
                .Build()
            .Lambda(std::move(lambda))
            .Done();

        if (itemType->GetKind() == ETypeAnnotationKind::Variant) {
            return Build<TCoRight>(ctx, node.Pos())
                .Input(std::move(map))
                .Done();
        }

        auto output = Build<TYtflowOutput>(ctx, node.Pos())
            .Operation(std::move(map))
            .OutputIndex()
                .Value(0)
                .Build()
            .Done();

        return output;
    }

    TExprNode::TPtr BuildPassthroughSourceMapOutput(
        const TYtflowReadWrap& readWrap,
        TPositionHandle position,
        TExprContext& ctx
    ) {
        auto type = readWrap.Ref().GetTypeAnn();
        YQL_ENSURE(type);

        if (!EnsureListType(readWrap.Ref(), ctx)) {
            ctx.AddError(TIssue(ctx.GetPosition(position),
                TStringBuilder()
                    << "Expected list type, but got: " << *type));
            return {};
        }

        auto* itemType = type->Cast<TListExprType>()->GetItemType();
        if (itemType->GetKind() != ETypeAnnotationKind::Struct) {
            ctx.AddError(TIssue(ctx.GetPosition(position),
                TStringBuilder()
                    << "Expected struct type, but got: " << *type));
            return {};
        }

        TSyncMap syncList;
        auto source = BuildOperationSource(
            readWrap.Ptr(), syncList, ctx, *State_->Types);

        auto sink = Build<TYtflowIntermediateSink>(ctx, TPositionHandle{})
            .Name()
                .Value("")
                .Build()
            .OutputIndex()
                .Value(0)
                .Build()
            .RowType(ExpandType(position, *itemType, ctx))
            .Done();

        auto sourceMap = Build<TYtflowSourceMap>(ctx, position)
            .World(MakeSyncNodeFromSyncList(syncList, position, ctx))
            .Sources()
                .Add(std::move(source))
                .Build()
            .Sinks()
                .Add(std::move(sink))
                .Build()
            .Settings()
                .Build()
            .Lambda()
                .Args({"stream"})
                .Body("stream")
                .Build()
            .Done().Ptr();

        return Build<TYtflowOutput>(ctx, position)
            .Operation(std::move(sourceMap))
            .OutputIndex()
                .Value(0)
                .Build()
            .Done().Ptr();
    }

    TMaybeNode<TExprBase> Extend(TExprBase node, TExprContext& ctx) {
        auto extend = node.Cast<TCoExtend>();

        for (auto input : extend) {
            if (!IsYtflowProviderInput(input.Ref())) {
                return node;
            }
        }

        auto type = node.Ref().GetTypeAnn();
        if (!type) {
            return node;
        }

        if (!EnsureListType(node.Ref(), ctx)) {
            return {};
        }

        auto* itemType = type
            ->Cast<TListExprType>()
            ->GetItemType();
        if (!EnsureStructType(node.Pos(), *itemType, ctx)) {
            return {};
        }

        TSyncMap syncList;
        TVector<TExprNode::TPtr> sources;
        sources.reserve(extend.Ref().ChildrenSize());
        for (auto input : extend) {
            auto sourceInput = input.Ptr();
            if (auto maybeReadWrap = input.Maybe<TYtflowReadWrap>()) {
                sourceInput = BuildPassthroughSourceMapOutput(
                    maybeReadWrap.Cast(), input.Pos(), ctx);
                if (!sourceInput) {
                    return {};
                }
            }

            sources.push_back(BuildOperationSource(
                sourceInput, syncList, ctx, *State_->Types));
        }

        auto sink = Build<TYtflowIntermediateSink>(ctx, TPositionHandle{})
            .Name()
                .Value("")
                .Build()
            .OutputIndex()
                .Value(0)
                .Build()
            .RowType(ExpandType(node.Pos(), *itemType, ctx))
            .Done();

        auto operation = Build<TYtflowExtend>(ctx, node.Pos())
            .World(MakeSyncNodeFromSyncList(syncList, node.Pos(), ctx))
            .Sources()
                .Add(std::move(sources))
                .Build()
            .Sinks()
                .Add(std::move(sink))
                .Build()
            .Settings()
                .Build()
            .Lambda()
                .Args({"stream"})
                .Body("stream")
                .Build()
            .GroupByColumns()
                .Add<TCoAtom>()
                    .Value(YTFLOW_INPUT_MESSAGE_ID_FIELD)
                    .Build()
                .Build()
            .Done();

        return Build<TYtflowOutput>(ctx, node.Pos())
            .Operation(std::move(operation))
            .OutputIndex()
                .Value(0)
                .Build()
            .Done();
    }

    TMaybeNode<TExprBase> EquiJoin(TExprBase node, TExprContext& ctx) {
        auto equiJoin = node.Cast<TCoEquiJoin>();
        YQL_ENSURE(equiJoin.ArgCount() >= 4);

        auto type = node.Ref().GetTypeAnn();
        if (!type) {
            return node;
        }

        if (type->GetKind() != ETypeAnnotationKind::List) {
            return node;
        }

        auto* itemType = type->Cast<TListExprType>()->GetItemType();
        if (itemType->GetKind() != ETypeAnnotationKind::Struct) {
            return node;
        }

        auto equiJoinTuple = equiJoin
            .Arg(equiJoin.ArgCount() - 2)
            .Cast<TCoEquiJoinTuple>();

        auto inputsInfo = CollectEquiJoinInputsInfoByLabel(equiJoinTuple.Ref());

        auto joinInputsByLabelResult = CollectEquiJoinInputsByLabel(
            node.Ref(),
            inputsInfo.JoinKeyColumnsByLabel,
            inputsInfo.RowSelectionModeByLabel,
            ctx,
            *State_->Types);

        if (!joinInputsByLabelResult.IsYtflowProviderBoundEquiJoin) {
            return node;
        }

        if (joinInputsByLabelResult.HasErrors) {
            return {};
        }

        const auto& streamInputsByLabel = joinInputsByLabelResult.StreamInputsByLabel;
        const auto& lookupSourceInputsByLabel = joinInputsByLabelResult.LookupSourceInputsByLabel;

        if (streamInputsByLabel.size() != 1) {
            auto message = TStringBuilder()
                << "Expected one stream, but got " << streamInputsByLabel.size();

            auto fillLabels = [](const auto& inputsByLabel) {
                TVector<TStringBuf> labels;
                for (const auto& [label, _] : inputsByLabel) {
                    labels.push_back(label);
                }
                return labels;
            };

            if (streamInputsByLabel.size() > 1) {
                auto labels = fillLabels(streamInputsByLabel);
                message << " with correlation names: " << JoinSeq(", ", labels);
            } else {
                auto labels = fillLabels(lookupSourceInputsByLabel);
                message << " (lookup sources: " << JoinSeq(", ", labels) << ")";
            }

            ctx.AddError(TIssue(
                ctx.GetPosition(node.Pos()),
                message));

            return {};
        }

        if (lookupSourceInputsByLabel.size() < 1) {
            ctx.AddError(TIssue(
                ctx.GetPosition(node.Pos()),
                TStringBuilder()
                    << "Expected one or more lookup sources, but got "
                    << lookupSourceInputsByLabel.size()));

            return {};
        }

        auto& joinOptionsNode = equiJoin.Arg(equiJoin.ArgCount() - 1).MutableRef();

        TJoinOptions joinOptions;
        if (auto status = ValidateEquiJoinOptions(
                node.Pos(), joinOptionsNode, joinOptions, ctx);
            status != TStatus::Ok
        ) {
            return {};
        }

        if (!joinOptions.PreferredSortSets.empty()) {
            ctx.AddError(TIssue(
                ctx.GetPosition(node.Pos()),
                TStringBuilder()
                    << "Preferred sort sets option for join is not supported"));

            return {};
        }

        if (joinOptions.StrictKeys) {
            ctx.AddError(TIssue(
                ctx.GetPosition(node.Pos()),
                TStringBuilder()
                    << "Strict keys option for join is not supported"));

            return {};
        }

        if (joinOptions.Flatten) {
            ctx.AddError(TIssue(
                ctx.GetPosition(node.Pos()),
                TStringBuilder()
                    << "Flatten option for join is not supported"));

            return {};
        }

        auto joinResultNode = BuildJoinNodeFromEquiJoinTuple(
            equiJoin.Arg(equiJoin.ArgCount() - 2).Ref(),
            joinInputsByLabelResult.ColumnTypes,
            streamInputsByLabel,
            lookupSourceInputsByLabel,
            inputsInfo.RowSelectionModeByLabel,
            node.Pos(),
            ctx,
            *State_->Types);

        if (!joinResultNode) {
            return {};
        }

        auto joinRenameMap = LoadJoinRenameMap(joinOptionsNode);

        TSyncMap syncList;
        auto source = BuildOperationSource(
            joinResultNode, syncList, ctx, *State_->Types);

        const ui32 outputIndex = 0;
        auto sink = Build<TYtflowIntermediateSink>(ctx, TPositionHandle{})
            .Name()
                .Value("")
                .Build()
            .OutputIndex()
                .Value(outputIndex)
                .Build()
            .RowType(ExpandType(node.Pos(), *itemType, ctx))
            .Done();

        auto joinRenameLambda = BuildJoinRenameLambda(
            node.Pos(), joinRenameMap, *itemType->Cast<TStructExprType>(), ctx);

        auto renameMap = Build<TYtflowMap>(ctx, node.Pos())
            .World(MakeSyncNodeFromSyncList(syncList, node.Pos(), ctx))
            .Sources()
                .Add(std::move(source))
                .Build()
            .Sinks()
                .Add(std::move(sink))
                .Build()
            .Settings()
                .Build()
            .Lambda()
                .Args({"stream"})
                .Body<TCoMap>()
                    .Input("stream")
                    .Lambda(std::move(joinRenameLambda))
                    .Build()
                .Build()
            .Done();

        auto output = Build<TYtflowOutput>(ctx, node.Pos())
            .Operation(std::move(renameMap))
            .OutputIndex()
                .Value(outputIndex)
                .Build()
            .Done().Ptr();

        return output;
    }

    TMaybeNode<TExprBase> WriteWrapWithReadWrap(TExprBase node, TExprContext& ctx) {
        auto writeWrap = node.Cast<TYtflowWriteWrap>();
        auto providerWrite = writeWrap.Input();

        auto* ytflowIntegration = GetYtflowIntegration(providerWrite.Ref(), *State_->Types);
        YQL_ENSURE(ytflowIntegration, "Unknown provider write: " << providerWrite.Ref().Content());

        auto writeContent = ytflowIntegration->GetWriteContent(providerWrite.Ref(), ctx);

        auto maybeReadWrap = TMaybeNode<TYtflowReadWrap>(writeContent);
        if (!maybeReadWrap) {
            return node;
        }

        auto type = writeContent->GetTypeAnn();
        if (!type) {
            return node;
        }

        auto output = BuildPassthroughSourceMapOutput(
            maybeReadWrap.Cast(), node.Pos(), ctx);
        if (!output) {
            return {};
        }

        auto newProviderWrite = ytflowIntegration->UpdateWriteContent(
            providerWrite.Ptr(), output, ctx);

        return Build<TYtflowWriteWrap>(ctx, writeWrap.Pos())
            .InitFrom(writeWrap)
            .Input(newProviderWrite)
            .Done();
    }

    TMaybeNode<TExprBase> Aggregate(TExprBase node, TExprContext& ctx) {
        auto aggregate = node.Cast<TCoAggregate>();
        auto input = aggregate.Input();

        auto inputType = input.Ref().GetTypeAnn();
        if (!inputType) {
            return node;
        }

        if (inputType->GetKind() != ETypeAnnotationKind::List) {
            return node;
        }

        auto* inputItemType = inputType->Cast<TListExprType>()->GetItemType();
        if (inputItemType->GetKind() != ETypeAnnotationKind::Struct) {
            return node;
        }

        auto outputType = aggregate.Ref().GetTypeAnn();
        if (!outputType) {
            return node;
        }

        if (outputType->GetKind() != ETypeAnnotationKind::List) {
            return node;
        }

        auto* outputItemType = outputType->Cast<TListExprType>()->GetItemType();
        if (outputItemType->GetKind() != ETypeAnnotationKind::Struct) {
            return node;
        }

        if (!IsYtflowProviderInput(input.Ref())) {
            return node;
        }

        auto maybeHopTraits = NHopping::ExtractHopTraits(
            aggregate,
            ctx,
            /*analyticsMode*/ false);

        if (!maybeHopTraits.Defined()) {
            return {};
        }

        auto hopTraits = maybeHopTraits.GetRef();

        auto hopSetting = GetSetting(aggregate.Settings().Ref(), "hopping");
        if (!NHopping::IsLegacyHopping(hopSetting)) {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
                << "Aggregate with new style hopping is not supported"));

            return {};
        }

        auto settings = RemoveSetting(aggregate.Settings().Ref(), "hopping", ctx);
        if (!EnsureValidSettings(*settings, {}, {}, ctx)) {
            return {};
        }

        NHopping::EnsureNotDistinct(aggregate);

        auto* inputStructItemType = inputItemType->Cast<TStructExprType>();

        NHopping::TKeysDescription keysDescription(
            *inputStructItemType,
            aggregate.Keys(),
            hopTraits.Column);

        if (keysDescription.NeedPickle()) {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
                << "Aggregate with composite key types is not supported yet"));

            return {};
        }

        auto keyExtractorLambda = keysDescription.GetKeySelector(
            ctx, node.Pos(), inputStructItemType);

        auto timeExtractorLambda = NHopping::BuildTimeExtractor(hopTraits.Traits, ctx);

        auto initLambda = NHopping::BuildInitHopLambda(aggregate, ctx);
        auto updateLambda = NHopping::BuildUpdateHopLambda(aggregate, ctx);
        auto saveLambda = NHopping::BuildSaveHopLambda(aggregate, ctx);
        auto loadLambda = NHopping::BuildLoadHopLambda(aggregate, ctx);
        auto mergeLambda = NHopping::BuildMergeHopLambda(aggregate, ctx);
        auto finishLambda = NHopping::BuildFinishHopLambda(
            aggregate,
            keysDescription.GetActualGroupKeys(),
            hopTraits.Column,
            ctx);

        TExprNode::TPtr buildCombineOutputLambda;
        const TTypeAnnotationNode* combineKeyType = nullptr;

        {
            auto keyArgument = ctx.NewArgument(node.Pos(), "key");
            auto savedStateArgument = ctx.NewArgument(node.Pos(), "savedState");

            TVector<TExprBase> outputItems;

            const auto& keysList = keysDescription.GetKeysList(ctx, node.Pos());
            if (keysList.size() > 1) {
                TVector<const TTypeAnnotationNode*> keyTypes;

                for (const auto& [index, key] : Enumerate(keysList)) {
                    outputItems.push_back(Build<TCoNameValueTuple>(ctx, node.Pos())
                        .Name(key)
                        .Value<TCoNth>()
                            .Tuple(keyArgument)
                            .Index()
                                .Value(index)
                                .Build()
                            .Build()
                        .Done());

                    keyTypes.push_back(inputStructItemType->FindItemType(key));
                }

                combineKeyType = ctx.MakeType<TTupleExprType>(keyTypes);
            } else {
                outputItems.push_back(Build<TCoNameValueTuple>(ctx, node.Pos())
                    .Name(keysList[0])
                    .Value(keyArgument)
                    .Done());

                combineKeyType = inputStructItemType->FindItemType(keysList[0]);
            }

            outputItems.push_back(Build<TCoNameValueTuple>(ctx, node.Pos())
                .Name()
                    .Value(YTFLOW_COMBINED_STATE_FIELD)
                    .Build()
                .Value(savedStateArgument)
                .Done());

            buildCombineOutputLambda = Build<TCoLambda>(ctx, node.Pos())
                .Args({keyArgument, savedStateArgument})
                .Body<TCoAsStruct>()
                    .Add(outputItems)
                    .Build()
                .Done().Ptr();
        }

        const TTypeAnnotationNode* combinePayloadType = nullptr;
        const TTypeAnnotationNode* combineItemType = nullptr;
        const TTypeAnnotationNode* combineSavedStateType = nullptr;

        {
            auto initLambdaClone = ctx.CopyLambdaWithTypes(*initLambda);

            if (!UpdateLambdaAllArgumentsTypes(initLambdaClone, {inputStructItemType}, ctx)) {
                return {};
            }

            if (!InstantAnnotateTypes(
                initLambdaClone,
                ctx,
                /*wholeProgram*/ false,
                *State_->Types
            )) {
                return {};
            }

            if (!initLambdaClone->GetTypeAnn()) {
                return {};
            }

            combinePayloadType = initLambdaClone->GetTypeAnn();

            auto saveLambdaClone = ctx.CopyLambdaWithTypes(*saveLambda);

            if (!UpdateLambdaAllArgumentsTypes(
                saveLambdaClone,
                {combinePayloadType},
                ctx
            )) {
                return {};
            }

            if (!InstantAnnotateTypes(
                saveLambdaClone,
                ctx,
                /*wholeProgram*/ false,
                *State_->Types
            )) {
                return {};
            }

            if (!saveLambdaClone->GetTypeAnn()) {
                return {};
            }

            TVector<const TItemExprType*> combineItemTypes;

            for (const auto& key : keysDescription.MemberKeys) {
                combineItemTypes.push_back(ctx.MakeType<TItemExprType>(
                    key,
                    inputStructItemType->FindItemType(key)));
            }

            auto combineStateItemTypes = TVector<const TTypeAnnotationNode*>{
                ctx.MakeType<TDataExprType>(EDataSlot::Timestamp),
                saveLambdaClone->GetTypeAnn()
            };

            combineSavedStateType = ctx.MakeType<TListExprType>(
                ctx.MakeType<TTupleExprType>(combineStateItemTypes));

            combineItemTypes.push_back(ctx.MakeType<TItemExprType>(
                YTFLOW_COMBINED_STATE_FIELD,
                combineSavedStateType));

            combineItemType = ctx.MakeType<TStructExprType>(combineItemTypes);
        }

        // NOTE: combineLambda gets following items:
        //   * stream of original items
        // and produces:
        //   * stream of key + aggregationState

        auto combineLambda = BuildLambdaFromSExprFactory(
            R"((
            (let factory (lambda '(
                    combineKeyType
                    combinePayloadType
                    column
                    hop
                    hopFrameCount
                    keyExtractorLambda
                    buildCombineOutputLambda
                    initLambda
                    updateLambda
                    saveLambda)
                (lambda '(stream) (block '(
                    (let innerDictType (DictType
                        (DataType 'Timestamp)
                        combinePayloadType))
                    (let stateStream (Map
                        (YtflowChunkedForwardList stream)
                        (lambda '(list) (Fold
                            list
                            (block '(
                                (let innerLinearStateType (TypeOf
                                    (ToDynamicLinear (ToMutDict
                                    (Dict innerDictType)
                                    (DependsOn list)))))
                                (let outerDict (Dict (DictType
                                    combineKeyType
                                    innerLinearStateType)))
                                (let outerLinearState (ToMutDict
                                    outerDict (DependsOn list)))
                                (return (ToDynamicLinear outerLinearState))))
                            (lambda '(item state) (block '(
                                (let outerLinearState (FromDynamicLinear state))
                                (let key (Apply keyExtractorLambda item))
                                (let outerLookupResult (MutDictLookup
                                    outerLinearState key))
                                (let outerLinearState (Nth outerLookupResult '0))
                                (let optionalInnerLinearState
                                    (Nth outerLookupResult '1))
                                (let innerLinearState (If
                                    (Exists optionalInnerLinearState)
                                    (Unwrap optionalInnerLinearState)
                                    (ToDynamicLinear (ToMutDict
                                        (Dict innerDictType)
                                        (DependsOn item)))))
                                (let time (Member item column))
                                (let hopStartTime (Unwrap (Sub
                                    time
                                    (SafeCast
                                        (Unwrap (Mod
                                            (BitCast time 'Int64)
                                            (BitCast (Interval hop) 'Int64)))
                                        (DataType 'Interval)))))
                                (let hopFrameIndexList (ListFromRange
                                    (Int64 '0) (Int64 hopFrameCount) (Int64 '1)))
                                (let updatedState (Fold
                                    hopFrameIndexList
                                    '(item innerLinearState hopStartTime)
                                    (lambda '(hopFrameIndex state) (block '(
                                        (let item (Nth state '0))
                                        (let innerLinearState (FromDynamicLinear
                                            (Nth state '1)))
                                        (let hopFrameStartTime (Nth state '2))
                                        (let innerLookupResult (MutDictLookup
                                            innerLinearState hopFrameStartTime))
                                        (let innerLinearState (Nth innerLookupResult '0))
                                        (let optionalHopFrameState
                                            (Nth innerLookupResult '1))
                                        (let hopFrameState (If
                                            (Exists optionalHopFrameState)
                                            (Apply updateLambda item
                                                (Unwrap optionalHopFrameState))
                                            (Apply initLambda item)))
                                        (let updatedInnerLinearState (ToDynamicLinear
                                            (MutDictUpsert
                                                innerLinearState
                                                hopFrameStartTime
                                                hopFrameState)))
                                        (let updatedHopFrameStartTime (Unwrap (Sub
                                            hopFrameStartTime (Interval hop))))
                                        (return '(
                                            item
                                            updatedInnerLinearState
                                            updatedHopFrameStartTime)))))))
                                (let innerLinearState (Nth updatedState '1))
                                (let outerLinearState (MutDictUpsert
                                    outerLinearState key innerLinearState))
                                (return (ToDynamicLinear outerLinearState)))))))))
                    (return (FlatMap
                        stateStream
                        (lambda '(item) (FlatMap
                            (DictItems (FromMutDict (FromDynamicLinear item)))
                            (lambda '(outerDictItem) (block '(
                                (let key (Nth outerDictItem '0))
                                (let innerDict (Nth outerDictItem '1))
                                (let savedInnerDict (Map
                                    (DictItems (FromMutDict (FromDynamicLinear innerDict)))
                                    (lambda '(innerDictItem) (block '(
                                        (let hopFrameStartTime (Nth innerDictItem '0))
                                        (let hopFrameState (Nth innerDictItem '1))
                                        (return '(
                                            hopFrameStartTime
                                            (Apply saveLambda hopFrameState))))))))
                                (let combineOutput (Apply
                                    buildCombineOutputLambda key savedInnerDict))
                                (return (AsList combineOutput))))))))))))))
            (return factory)
            ))",
            {
                ExpandType(node.Pos(), *combineKeyType, ctx),
                ExpandType(node.Pos(), *combinePayloadType, ctx),
                ctx.NewAtom(node.Pos(), hopTraits.Column),
                ctx.NewAtom(node.Pos(), ToString(hopTraits.Hop)),
                ctx.NewAtom(node.Pos(), ToString(hopTraits.Interval / hopTraits.Hop)),
                keyExtractorLambda,
                buildCombineOutputLambda,
                initLambda,
                updateLambda,
                saveLambda,
            },
            node.Pos(),
            ctx);

        // build pre map with combine & hopTraits.Column evaluation
        TExprNode::TPtr combineMapOutput;

        {
            TSyncMap timeExtractorMapSyncList;
            auto timeExtractorMapSource = BuildOperationSource(
                input.Ptr(), timeExtractorMapSyncList, ctx, *State_->Types);

            auto structItems = inputStructItemType->GetItems();

            auto columnType = hopTraits.Traits.TimeExtractor().Ref().GetTypeAnn();
            const TTypeAnnotationNode* unwrappedColumnType = columnType->IsOptionalOrNull()
                ? columnType->Cast<TOptionalExprType>()->GetItemType()
                : columnType;

            structItems.push_back(ctx.MakeType<TItemExprType>(
                hopTraits.Column,
                unwrappedColumnType));

            auto* extendedItemType = ctx.MakeType<TStructExprType>(
                std::move(structItems));

            auto timeExtractorMapSink = Build<TYtflowIntermediateSink>(ctx, TPositionHandle{})
                .Name()
                    .Value("")
                    .Build()
                .OutputIndex()
                    .Value(0)
                    .Build()
                .RowType(ExpandType(node.Pos(), *extendedItemType, ctx))
                .Done().Ptr();

            auto timeExtractorMap = ctx.Builder(node.Pos())
                .Callable(
                    TMaybeNode<TYtflowPersistentSource>(timeExtractorMapSource)
                        ? TYtflowSourceMap::CallableName()
                        : TYtflowMap::CallableName())
                    .Add(0, MakeSyncNodeFromSyncList(
                        timeExtractorMapSyncList, node.Pos(), ctx))
                    .List(1)
                        .Add(0, std::move(timeExtractorMapSource))
                        .Seal()
                    .List(2)
                        .Add(0, std::move(timeExtractorMapSink))
                        .Seal()
                    .List(3)
                        .List(0)
                            .Atom(0, "inject_input_message_id")
                            .Atom(1, "")
                            .Seal()
                        .Seal()
                    .Lambda(4)
                        .Param("stream")
                        .Callable(0, TCoOrderedFlatMap::CallableName())
                            .Arg(0, "stream")
                            .Lambda(1)
                                .Param("item")
                                .Callable(0, TCoJust::CallableName())
                                    .Callable(0, TCoAddMember::CallableName())
                                        .Arg(0, "item")
                                        .Atom(1, hopTraits.Column)
                                        .Callable(2, TCoUnwrap::CallableName())
                                            .Apply(0, timeExtractorLambda)
                                                .With(0, "item")
                                                .Seal()
                                            .Seal()
                                        .Seal()
                                    .Seal()
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal()
                .Build();

            auto timeExtractorMapOutput = Build<TYtflowOutput>(ctx, node.Pos())
                .Operation(TYtflowMapBase(timeExtractorMap))
                .OutputIndex()
                    .Value(0)
                    .Build()
                .Done().Ptr();

            TSyncMap combineMapSyncList;
            auto combineMapSource = BuildOperationSource(
                timeExtractorMapOutput, combineMapSyncList, ctx, *State_->Types);

            auto combineMapSink = Build<TYtflowIntermediateSink>(ctx, TPositionHandle{})
                .Name()
                    .Value("")
                    .Build()
                .OutputIndex()
                    .Value(0)
                    .Build()
                .RowType(ExpandType(node.Pos(), *combineItemType, ctx))
                .Done().Ptr();

            auto combineMap = Build<TYtflowTransformMap>(ctx, node.Pos())
                .World(MakeSyncNodeFromSyncList(combineMapSyncList, node.Pos(), ctx))
                .Sources()
                    .Add(std::move(combineMapSource))
                    .Build()
                .Sinks()
                    .Add(std::move(combineMapSink))
                    .Build()
                .Settings()
                    .Build()
                .Lambda(TCoLambda(combineLambda))
                .GroupByColumns()
                    .Add<TCoAtom>()
                        .Value(YTFLOW_INPUT_MESSAGE_ID_FIELD)
                        .Build()
                    .Build()
                .Done();

            combineMapOutput = Build<TYtflowOutput>(ctx, node.Pos())
                .Operation(combineMap)
                .OutputIndex()
                    .Value(0)
                    .Build()
                .Done().Ptr();
        }

        // NOTE: updateStateLambda gets following items:
        //   * stream of key + aggregationState
        //   * old aggregation state (List<Tuple<hop_start_time, hop_aggregation_state>>)
        // and produces:
        //   * new aggregation state with same schema
        //   * list of tuples (event timestamp, trigger timestamp) trigger timestamps for newly added timers

        auto updateStateLambda = BuildLambdaFromSExprFactory(
            R"((
            (let factory (lambda '(
                    timerDelay
                    combinedStateField
                    loadLambda
                    mergeLambda
                    saveLambda)
                (lambda '(stream savedState) (block '(
                    (let compositeStateStream (Condense
                        stream
                        (block '(
                            (let loadedState (Map savedState (lambda '(item) '(
                                (Nth item '0) (Apply loadLambda (Nth item '1))))))
                            (let linearState (ToMutDict
                                (ToDict
                                    loadedState
                                    (lambda '(item) (Nth item '0))
                                    (lambda '(item) (Nth item '1))
                                    '('Hashed 'One))
                                (DependsOn stream)
                                (DependsOn savedState)))
                            (let linearState (ToDynamicLinear linearState))
                            (let emptyList (List (ListType (TupleType
                                (DataType 'Timestamp)
                                (DataType 'Timestamp)))))
                            (return '(linearState emptyList))))
                        (lambda '(item compositeState) (Bool 'false))
                        (lambda '(item compositeState) (Fold
                            (Member item combinedStateField)
                            compositeState
                            (lambda '(combinedStateItem compositeState) (block '(
                                (let linearState (FromDynamicLinear
                                    (Nth compositeState '0)))
                                (let timerTimestamps (Nth compositeState '1))
                                (let hopFrameStartTime (Nth combinedStateItem '0))
                                (let hopFrameCombinedState (Apply loadLambda
                                    (Nth combinedStateItem '1)))
                                (let lookupResult (MutDictLookup
                                    linearState hopFrameStartTime))
                                (let linearState (Nth lookupResult '0))
                                (let optionalHopFrameState (Nth lookupResult '1))
                                (let hopFrameState (If
                                    (Exists optionalHopFrameState)
                                    (Apply
                                        mergeLambda
                                        hopFrameCombinedState
                                        (Unwrap optionalHopFrameState))
                                    hopFrameCombinedState))
                                (let updatedLinearState (ToDynamicLinear (MutDictUpsert
                                    linearState hopFrameStartTime hopFrameState)))
                                (let updatedTimerTimestamps (If
                                    (Exists optionalHopFrameState)
                                    timerTimestamps
                                    (Append
                                        timerTimestamps
                                        '(
                                            (Unwrap (Add
                                                hopFrameStartTime
                                                (Interval timerDelay)))
                                            hopFrameStartTime))))
                                (return '(
                                    updatedLinearState updatedTimerTimestamps)))))))))
                    (let compositeState (Unwrap (ToOptional (ForwardList
                        compositeStateStream))))
                    (let linearState (Nth compositeState '0))
                    (let timerTimestamps (Nth compositeState '1))
                    (let state (DictItems (FromMutDict (FromDynamicLinear linearState))))
                    (let newSavedState (Map state (lambda '(item) '(
                        (Nth item '0) (Apply saveLambda (Nth item '1))))))
                    (return '(newSavedState timerTimestamps)))))))
            (return factory)
            )
        )", {
            ctx.NewAtom(node.Pos(), ToString(hopTraits.Interval + hopTraits.Delay)),
            ctx.NewAtom(node.Pos(), YTFLOW_COMBINED_STATE_FIELD),
            loadLambda,
            mergeLambda,
            saveLambda,
        }, node.Pos(), ctx);

        // NOTE: postprocessLambda gets following items:
        //   * key
        //   * aggregation state (List<Tuple<hop_start_time, hop_aggregation_state>>)
        //   * max_hop_start_time (for window close)
        // and produces tuple of:
        //   * stream with triggered postprocessed frames (stream allows for further composition)
        //   * list with remaining frames
        //   * flag whether state should be removed

        auto postprocessLambda = BuildLambdaFromSExprFactory(
            R"((
            (let factory (lambda '(
                    interval
                    loadLambda
                    finishLambda)
                (lambda '(key savedState maxHopStartTime) (block '(
                    (let triggeredFrames (OrderedFilter
                        savedState
                        (lambda '(item) (LessOrEqual (Nth item '0) maxHopStartTime))))
                    (let remainingFrames (OrderedFilter
                        savedState
                        (lambda '(item) (Greater (Nth item '0) maxHopStartTime))))
                    (let postprocessedFrames (Map
                        triggeredFrames
                        (lambda '(item) (block '(
                            (let state (Apply loadLambda (Nth item '1)))
                            (let time (Nth item '0))
                            (let time (Add time (Interval interval)))
                            (let finalItem (Apply finishLambda key state time))
                            (return finalItem))))))
                    (return '(
                        (Iterator postprocessedFrames)
                        remainingFrames
                        (Not (HasItems remainingFrames)))))))))
            (return factory)
            ))",
            {
                ctx.NewAtom(node.Pos(), ToString(hopTraits.Interval)),
                loadLambda,
                finishLambda,
            },
            node.Pos(),
            ctx);

        TSyncMap syncList;
        auto source = BuildOperationSource(
            combineMapOutput, syncList, ctx, *State_->Types);

        auto sink = Build<TYtflowIntermediateSink>(ctx, TPositionHandle{})
            .Name()
                .Value("")
                .Build()
            .OutputIndex()
                .Value(0)
                .Build()
            .RowType(ExpandType(node.Pos(), *outputItemType, ctx))
            .Done();

        auto hoppingAggregate = Build<TYtflowHoppingAggregate>(ctx, node.Pos())
            .World(MakeSyncNodeFromSyncList(syncList, node.Pos(), ctx))
            .Sources()
                .Add(std::move(source))
                .Build()
            .Sinks()
                .Add(std::move(sink))
                .Build()
            .Settings()
                .Build()
            .Keys(
                MakeAtomList(node.Pos(), keysDescription.GetActualGroupKeys(), ctx))
            .UpdateStateLambda(std::move(updateStateLambda))
            .PostprocessLambda(std::move(postprocessLambda))
            .Hop()
                .Value(hopTraits.Hop)
                .Build()
            .Interval()
                .Value(hopTraits.Interval)
                .Build()
            .Delay()
                .Value(hopTraits.Delay)
                .Build()
            .SavedStateType(ExpandType(node.Pos(), *combineSavedStateType, ctx))
            .Done();

        auto output = Build<TYtflowOutput>(ctx, node.Pos())
            .Operation(hoppingAggregate.Ptr())
            .OutputIndex()
                .Value(0)
                .Build()
            .Done();

        return output;
    }

private:
    TYtflowState::TPtr State_;
};


THolder<IGraphTransformer> CreateYtflowPhysicalOptProposalTransformer(TYtflowState::TPtr state) {
    return MakeHolder<TYtflowPhysicalOptProposalTransformer>(std::move(state));
}

} // namespace NYql
