#include "yql_ytflow_provider_impl.h"
#include "yql_ytflow_constants.h"
#include "yql_ytflow_join_utils.h"
#include "yql_ytflow_utils.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_join.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>
#include <yt/yql/providers/ytflow/integration/interface/yql_ytflow_integration.h>

#include <library/cpp/iterator/zip.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/cast.h>
#include <util/string/join.h>


namespace NYql {

using namespace NNodes;

const THashSet<TStringBuf> SPECIAL_GROUPBY_COLUMNS {
    YTFLOW_INPUT_MESSAGE_ID_FIELD
};


class TYtflowDataSinkTypeAnnotationTransformer: public TVisitorTransformerBase {
public:
    TYtflowDataSinkTypeAnnotationTransformer(TYtflowState::TPtr state)
        : TVisitorTransformerBase(true)
        , State_(std::move(state))
    {
#define ADD_HANDLER(nodeType, method) \
    AddHandler({nodeType::CallableName()}, Hndl(&TYtflowDataSinkTypeAnnotationTransformer::method))

        ADD_HANDLER(TYtflowWriteWrap, HandleWriteWrap);
        ADD_HANDLER(TYtflowPublish, HandlePublish);
        ADD_HANDLER(TYtflowIntermediateSink, HandleIntermediateSink);
        ADD_HANDLER(TYtflowPersistentSink, HandlePersistentSink);
        ADD_HANDLER(TYtflowOutput, HandleOutput);
        ADD_HANDLER(TYtflowSourceMap, HandleSourceMap);
        ADD_HANDLER(TYtflowTransformMap, HandleTransformMap);
        ADD_HANDLER(TYtflowSwiftMap, HandleSwiftMap);
        ADD_HANDLER(TYtflowExtend, HandleExtend);
        ADD_HANDLER(TYtflowMap, HandleMap);
        ADD_HANDLER(TYtflowLookupJoin, HandleLookupJoin);
        ADD_HANDLER(TYtflowChunkedForwardList, HandleChunkedForwardList);
        ADD_HANDLER(TYtflowHoppingAggregate, HandleHoppingAggregate);

#undef ADD_HANDLER
    }

private:
    TStatus HandleWriteWrap(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 1, 2, ctx)) {
            return TStatus::Error;
        }

        auto* inputChild = input->Child(TYtflowWriteWrap::idx_Input);
        if (!EnsureWorldType(*inputChild, ctx)) {
            return TStatus::Error;
        }

        if (input->ChildrenSize() > TYtflowReadWrap::idx_Token && !TCoSecureParam::Match(input->Child(TYtflowReadWrap::idx_Token))) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Expect SecureParam but got: "
                << input->Child(TYtflowReadWrap::idx_Token)->Content()));
            return TStatus::Error;
        }

        input->SetTypeAnn(inputChild->GetTypeAnn());

        return TStatus::Ok;
    }

    TStatus HandlePublish(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx)) {
            return TStatus::Error;
        }

        auto* world = input->Child(TYtflowPublish::idx_World);
        if (!EnsureWorldType(*world, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureValidSettings(*input->Child(TYtflowPublish::idx_Settings), {}, {}, ctx)) {
            return TStatus::Error;
        }

        input->SetTypeAnn(world->GetTypeAnn());

        return TStatus::Ok;
    }

    TStatus HandleSinkBase(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureMinArgsCount(*input, 2, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(TYtflowSinkBase::idx_Name), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(TYtflowSinkBase::idx_OutputIndex), ctx)) {
            return TStatus::Error;
        }

        return TStatus::Ok;
    }

    TStatus HandleIntermediateSink(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (auto status = HandleSinkBase(input, ctx); status != TStatus::Ok) {
            return status;
        }

        auto* rowType = input->Child(TYtflowIntermediateSink::idx_RowType);
        if (!EnsureTypeWithStructType(*rowType, ctx)) {
            return TStatus::Error;
        }

        auto* itemType = rowType->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        input->SetTypeAnn(ctx.MakeType<TListExprType>(itemType));

        return TStatus::Ok;
    }

    TStatus HandlePersistentSink(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (auto status = HandleSinkBase(input, ctx); status != TStatus::Ok) {
            return status;
        }

        if (!EnsureArgsCount(*input, 3, ctx)) {
            return TStatus::Error;
        }

        auto* inputChild = input->Child(TYtflowPersistentSink::idx_Input);
        auto writeWrapInputChild = TYtflowWriteWrap(inputChild).Input();
        auto* ytflowIntegration = GetYtflowIntegration(writeWrapInputChild.Ref(), *State_->Types);
        YQL_ENSURE(ytflowIntegration);

        auto content = ytflowIntegration->GetWriteContent(writeWrapInputChild.Ref(), ctx);
        if (!EnsureListType(*content, ctx)) {
            return TStatus::Error;
        }

        auto* itemType = content->GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        if (!EnsureStructType(content->Pos(), *itemType, ctx)) {
            return TStatus::Error;
        }

        input->SetTypeAnn(content->GetTypeAnn());

        return TStatus::Ok;
    }

    TStatus HandleOutput(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx)) {
            return TStatus::Error;
        }

        auto* operation = input->Child(TYtflowOutput::idx_Operation);
        if (!TYtflowOpBase::Match(operation)) {
            ctx.AddError(TIssue(
                ctx.GetPosition(input->Pos()),
                TStringBuilder()
                    << "Expected Ytflow operation, but got: " << operation->Content()));

            return TStatus::Error;
        }

        auto* outputIndexNode = input->Child(TYtflowOutput::idx_OutputIndex);
        if (!EnsureAtom(*outputIndexNode, ctx)) {
            return TStatus::Error;
        }

        auto operationSinks = operation->Child(TYtflowOpBase::idx_Sinks);

        ui32 outputIndex;
        if (!TryFromString<ui32>(outputIndexNode->Content(), outputIndex)
            || outputIndex > operationSinks->ChildrenSize()
        ) {
            ctx.AddError(TIssue(
                ctx.GetPosition(input->Pos()),
                TStringBuilder()
                    << "Bad " << TYtflowOutput::CallableName()
                    << " index value: " << outputIndexNode->Content()));

            return TStatus::Error;
        }

        input->SetTypeAnn(operationSinks->Child(outputIndex)->GetTypeAnn());

        return TStatus::Ok;
    }

    TStatus ValidateOpBase(
        const TExprNode::TPtr& input, TExprContext& ctx,
        TVector<const TTypeAnnotationNode*>& sourceItemTypes,
        TVector<const TTypeAnnotationNode*>& sinkItemTypes,
        ui32& maxSinkOutputIndex
    ) {
        if (!EnsureMinArgsCount(*input, 4, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureWorldType(*input->Child(TYtflowOpBase::idx_World), ctx)) {
            return TStatus::Error;
        }

        auto* sourcesNode = input->Child(TYtflowOpBase::idx_Sources);
        if (!EnsureMinArgsCount(*sourcesNode, 1, ctx)) {
            return TStatus::Error;
        }

        bool hasSourceIssues = false;

        for (const auto& child: sourcesNode->Children()) {
            if (!EnsureListType(child->Pos(), *child->GetTypeAnn(), ctx)) {
                hasSourceIssues = true;
                continue;
            }

            auto* childItemType = child->GetTypeAnn()->Cast<TListExprType>()->GetItemType();

            if (!EnsureStructType(child->Pos(), *childItemType, ctx)) {
                hasSourceIssues = true;
                continue;
            }

            sourceItemTypes.push_back(childItemType);
        }

        if (hasSourceIssues) {
            return TStatus::Error;
        }

        auto* sinksNode = input->Child(TYtflowOpBase::idx_Sinks);
        if (!EnsureMinArgsCount(*sinksNode, 1, ctx)) {
            return TStatus::Error;
        }

        bool hasSinkIssues = false;

        maxSinkOutputIndex = 0;
        THashMap<ui32, TVector<TExprNode::TPtr>> sinksByOutputIndex;
        for (const auto& child: sinksNode->Children()) {
            if (!EnsureListType(child->Pos(), *child->GetTypeAnn(), ctx)) {
                hasSinkIssues = true;
                continue;
            }

            auto* childItemType = child->GetTypeAnn()->Cast<TListExprType>()->GetItemType();

            if (!EnsureStructType(child->Pos(), *childItemType, ctx)) {
                hasSinkIssues = true;
                continue;
            }

            ui32 index = 0;
            auto outputIndexNode = child->Child(TYtflowSinkBase::idx_OutputIndex);
            if (!TryFromString(outputIndexNode->Content(), index)) {
                ctx.AddError(TIssue(ctx.GetPosition(outputIndexNode->Pos()), TStringBuilder()
                    << "Unexpected sink output index value: " << outputIndexNode->Content()));
                return TStatus::Error;
            }

            sinksByOutputIndex[index].push_back(child);

            maxSinkOutputIndex = std::max(maxSinkOutputIndex, index);
        }

        sinkItemTypes.resize(maxSinkOutputIndex + 1);
        for (const auto& [outputIndex, sinks] : sinksByOutputIndex) {
            THashSet<const TTypeAnnotationNode*> sinkTypes;
            for (const auto& sink : sinks) {
                sinkTypes.insert(sink->GetTypeAnn()->Cast<TListExprType>()->GetItemType());
            }

            const TTypeAnnotationNode* resultSinkType = *sinkTypes.begin();
            if (sinkTypes.size() != 1) {
                auto formatTypes = [](const auto& sinkItemTypes) {
                    TVector<TString> formattedTypes;
                    for (const auto* type : sinkItemTypes) {
                        formattedTypes.push_back(FormatType(type));
                    }
                    return formattedTypes;
                };

                ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder()
                    << "Got unexpected unique types count: "
                    << sinkTypes.size() << "(types: " << JoinSeq(", ", formatTypes(sinkTypes)) << ")"));
                return TStatus::Error;
            }

            sinkItemTypes[outputIndex] = resultSinkType;
        }

        if (hasSinkIssues) {
            return TStatus::Error;
        }

        TExprNode::TPtr middleStreamSort;
        VisitExpr(input, [&middleStreamSort, root = input](const TExprNode::TPtr& node) {
            if (node != root && (TMaybeNode<TYtflowOpBase>(node) || node->IsLambda())) {
                return false;
            }

            if (TMaybeNode<TCoSort>(node)) {
                middleStreamSort = node;
                return false;
            }

            return !middleStreamSort;
        });

        if (middleStreamSort) {
            ctx.AddError(TIssue(
                ctx.GetPosition(middleStreamSort->Pos()),
                "ORDER BY is not supported in subquery for ytflow engine"));
            return TStatus::Error;
        }

        return TStatus::Ok;
    }

    TStatus ValidateMapBase(
        const TExprNode::TPtr& input,
        TExprContext& ctx
    ) {
        if (!EnsureMinArgsCount(*input, 5, ctx)) {
            return TStatus::Error;
        }

        TVector<const TTypeAnnotationNode*> sourceItemTypes;
        TVector<const TTypeAnnotationNode*> sinkItemTypes;
        ui32 maxSinkOutputIndex = 0;

        if (auto status = ValidateOpBase(input, ctx, sourceItemTypes, sinkItemTypes, maxSinkOutputIndex);
            status != TStatus::Ok
        ) {
            return status;
        }

        const bool supportsExtendSetting =
            TYtflowTransformMap::Match(input.Get()) || TYtflowSwiftMap::Match(input.Get());

        THashSet<TStringBuf> supportedSettings = {INJECT_INPUT_MESSAGE_ID_SETTING};
        if (supportsExtendSetting) {
            supportedSettings.insert(EXTEND_SETTING);
        }

        if (!EnsureValidSettings(
            *input->Child(TYtflowMap::idx_Settings),
            supportedSettings,
            [](TStringBuf /*name*/, TExprNode& /*setting*/, TExprContext& /*ctx*/) {
                return true;
            },
            ctx
        )) {
            return TStatus::Error;
        }

        const bool hasExtendSetting = supportsExtendSetting && HasSetting(
            *input->Child(TYtflowMapBase::idx_Settings),
            EXTEND_SETTING);
        const bool hasExtendSemantics = TYtflowExtend::Match(input.Get()) || hasExtendSetting;

        if (!hasExtendSemantics && sourceItemTypes.size() > 1) {
            ctx.AddError(TIssue(
                ctx.GetPosition(input->Pos()),
                TStringBuilder()
                    << input->Content() << " doesn't support multiple inputs"));

            return TStatus::Error;
        }

        if (hasExtendSemantics) {
            auto* sourceItemType = sourceItemTypes.front();
            for (auto* itemType : sourceItemTypes) {
                if (itemType != sourceItemType) {
                    ctx.AddError(TIssue(
                        ctx.GetPosition(input->Pos()),
                        TStringBuilder()
                            << "Expected all input types to be equal, but got: "
                            << *sourceItemType << " and " << *itemType));

                    return TStatus::Error;
                }
            }
        }

        auto& lambda = input->ChildRef(TYtflowMap::idx_Lambda);
        if (auto status = ConvertToLambda(lambda, ctx, 1, 1); status != TStatus::Ok) {
            return status;
        }

        auto* sourceItemType = sourceItemTypes[0];
        auto* inputLambdaType = ctx.MakeType<TStreamExprType>(sourceItemType);

        if (!UpdateLambdaAllArgumentsTypes(lambda, {inputLambdaType}, ctx)) {
            return TStatus::Error;
        }

        if (!lambda->GetTypeAnn()) {
            return TStatus::Repeat;
        }

        if (!EnsureStreamType(*lambda, ctx)) {
            return TStatus::Error;
        }

        auto* outputItemType =
            lambda->GetTypeAnn()->Cast<TStreamExprType>()->GetItemType();

        TVector<const TTypeAnnotationNode*> outputItemTypes;
        if (outputItemType->GetKind() == ETypeAnnotationKind::Variant) {
            auto* underlyingType = outputItemType->Cast<TVariantExprType>()->GetUnderlyingType();
            if (!EnsureTupleType(lambda->Pos(), *underlyingType, ctx)) {
                return TStatus::Error;
            }

            auto* tupleType = underlyingType->Cast<TTupleExprType>();
            for (auto* type : tupleType->GetItems()) {
                if (!EnsureStructType(lambda->Pos(), *type, ctx)) {
                    return TStatus::Error;
                }
                outputItemTypes.push_back(type);
            }
        } else {
            outputItemTypes.push_back(outputItemType);
        }

        for (auto* sinkItemType : sinkItemTypes) {
            if (!EnsureStructType(lambda->Pos(), *sinkItemType, ctx)) {
                return TStatus::Error;
            }
        }

        if (outputItemTypes.size() != sinkItemTypes.size()) {
            ctx.AddError(TIssue(
                ctx.GetPosition(lambda->Pos()),
                TStringBuilder()
                    << "Expected " << sinkItemTypes.size()
                    << " lambda output types, but got "
                    << outputItemTypes.size()));
            return TStatus::Error;
        }

        if (maxSinkOutputIndex >= outputItemTypes.size()) {
            ctx.AddError(TIssue(ctx.GetPosition(lambda->Pos()), TStringBuilder()
                << "Unexpected sink output index value: " << maxSinkOutputIndex));
            return TStatus::Error;
        }

        const auto& sinksNode = input->Child(TYtflowOpBase::idx_Sinks);
        for (auto [outputItemType, sinkItemType, sink] : Zip(outputItemTypes, sinkItemTypes, sinksNode->Children())) {
            if (outputItemType != sinkItemType) {
                ctx.AddError(TIssue(
                    ctx.GetPosition(lambda->Pos()),
                    TStringBuilder()
                        << "Expected type: " << *sinkItemType
                        << ", but got: " << *outputItemType));

                return TStatus::Error;
            }
        }

        auto* resultType = [&outputItemTypes, &ctx]() -> const TTypeAnnotationNode* {
            if (outputItemTypes.size() == 1) {
                return outputItemTypes[0];
            } else {
                return ctx.MakeType<TVariantExprType>(ctx.MakeType<TTupleExprType>(outputItemTypes));
            }
        }();

        input->SetTypeAnn(ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            input->Child(TYtflowOpBase::idx_World)->GetTypeAnn(),
            ctx.MakeType<TListExprType>(resultType)
        }));

        // TODO(artemmashin): find out why input requires this restriction.
        // Without this the following error occurs: Rewrite error, missing Multi(0:{},1:{}) constraint in node Right!
        input->AddConstraint(ctx.MakeConstraint<TMultiConstraintNode>(
            NSorted::TSimpleMap<ui32, TConstraintSet>{
                { 0, {} },
                { 1, {} }
            }
        ));

        return TStatus::Ok;
    }

    TStatus HandleSourceMap(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 5, ctx)) {
            return TStatus::Error;
        }

        if (auto status = ValidateMapBase(input, ctx);
            status != TStatus::Ok
        ) {
            return status;
        }

        auto* sourcesNode = input->Child(TYtflowOpBase::idx_Sources);
        if (sourcesNode->ChildrenSize() != 1) {
            ctx.AddError(TIssue(
                ctx.GetPosition(input->Pos()),
                TStringBuilder()
                    << TYtflowSourceMap::CallableName()
                    << " doesn't support multiple inputs"));

            return TStatus::Error;
        }

        if (!EnsureSpecificCallable(
            *sourcesNode->Child(0), {TYtflowPersistentSource::CallableName()}, ctx
        )) {
            return TStatus::Error;
        }

        return TStatus::Ok;
    }

    TStatus HandleTransformMap(const TExprNode::TPtr& input, TExprContext& ctx) {
        return ValidatePhysicalMap<TYtflowTransformMap>(input, ctx);
    }

    TStatus HandleSwiftMap(const TExprNode::TPtr& input, TExprContext& ctx) {
        return ValidatePhysicalMap<TYtflowSwiftMap>(input, ctx);
    }

    template <typename TMap>
    TStatus ValidatePhysicalMap(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 6, ctx)) {
            return TStatus::Error;
        }

        if (auto status = ValidateMapBase(input, ctx);
            status != TStatus::Ok
        ) {
            return status;
        }

        if (auto status = ValidateGroupByColumns(
                input,
                TMap::idx_GroupByColumns,
                TMap::CallableName(),
                ctx
            );
            status != TStatus::Ok
        ) {
            return status;
        }

        return TStatus::Ok;
    }

    TStatus HandleExtend(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 6, ctx)) {
            return TStatus::Error;
        }

        if (auto status = ValidateMapBase(input, ctx);
            status != TStatus::Ok
        ) {
            return status;
        }

        return ValidateGroupByColumns(
            input,
            TYtflowExtend::idx_GroupByColumns,
            input->Content(),
            ctx
        );
    }

    TStatus ValidateGroupByColumns(
        const TExprNode::TPtr& input,
        size_t groupByColumnsIndex,
        TStringBuf callableName,
        TExprContext& ctx
    ) {
        auto* groupByColumnsNode = input->Child(groupByColumnsIndex);
        if (!EnsureTupleOfAtoms(*groupByColumnsNode, ctx)) {
            return TStatus::Error;
        }

        auto groupByColumns = ParseTupleOfAtoms(*groupByColumnsNode);
        if (groupByColumns.empty()) {
            ctx.AddError(TIssue(
                ctx.GetPosition(input->Pos()),
                TStringBuilder()
                    << callableName
                    << "Empty group by columns"));

            return TStatus::Error;
        }

        auto* sourceItemType = input
            ->Child(TYtflowOpBase::idx_Sources)
            ->Child(0)
            ->GetTypeAnn()
            ->Cast<TListExprType>()
            ->GetItemType()
            ->Cast<TStructExprType>();

        TVector<TString> unknownGroupByColumns;
        for (const auto& column : groupByColumns) {
            if (!sourceItemType->FindItem(column) &&
                !SPECIAL_GROUPBY_COLUMNS.contains(column)
            ) {
                unknownGroupByColumns.push_back(column);
            }
        }

        if (!unknownGroupByColumns.empty()) {
            ctx.AddError(TIssue(
                ctx.GetPosition(input->Pos()),
                TStringBuilder()
                    << "Unknown groupby columns: "
                    << JoinSeq(", ", unknownGroupByColumns)));

            return TStatus::Error;
        }

        return TStatus::Ok;
    }

    TStatus HandleMap(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 5, ctx)) {
            return TStatus::Error;
        }

        if (auto status = ValidateMapBase(input, ctx);
            status != TStatus::Ok
        ) {
            return status;
        }

        return TStatus::Ok;
    }

    TStatus HandleLookupJoin(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 6, ctx)) {
            return TStatus::Error;
        }

        auto* stream = input->Child(TYtflowLookupJoin::idx_Stream);
        if (!EnsureStreamType(*stream, ctx)) {
            return TStatus::Error;
        }

        auto* streamItemType = stream
            ->GetTypeAnn()
            ->Cast<TStreamExprType>()
            ->GetItemType();

        if (!EnsureStructType(stream->Pos(), *streamItemType, ctx)) {
            return TStatus::Error;
        }

        auto* lookupSource = input->Child(TYtflowLookupJoin::idx_LookupSource);
        if (!EnsureListType(*lookupSource, ctx)) {
            return TStatus::Error;
        }

        auto* lookupSourceItemType = lookupSource
            ->GetTypeAnn()
            ->Cast<TListExprType>()
            ->GetItemType();

        if (!EnsureStructType(lookupSource->Pos(), *lookupSourceItemType, ctx)) {
            return TStatus::Error;
        }

        TLookupJoinScope streamScope;
        auto* streamStructType = streamItemType->Cast<TStructExprType>();

        TLookupJoinScope lookupSourceScope;
        auto* lookupSourceStructType = lookupSourceItemType->Cast<TStructExprType>();

        if (!ValidateLookupJoinScopes(
                *input->Child(TYtflowLookupJoin::idx_StreamScope),
                streamStructType,
                *input->Child(TYtflowLookupJoin::idx_LookupSourceScope),
                lookupSourceStructType,
                input->Pos(),
                streamScope,
                lookupSourceScope,
                ctx
        )) {
            return TStatus::Error;
        }

        auto* joinKindNode = input->Child(TYtflowLookupJoin::idx_JoinKind);
        if (!EnsureAtom(*joinKindNode, ctx)) {
            return TStatus::Error;
        }

        auto joinKind = joinKindNode->Content();
        bool streamFromLeftSide = streamScope.IsLeftSide;

        if (!ValidateLookupJoinKind(
                joinKind,
                streamFromLeftSide,
                joinKindNode->Pos(),
                ctx
        )) {
            return TStatus::Error;
        }

        auto* columnsNode = input->Child(TYtflowLookupJoin::idx_Columns);
        if (!EnsureTupleOfAtoms(*columnsNode, ctx)) {
            return TStatus::Error;
        }

        auto columns = ParseTupleOfAtoms(*columnsNode);

        const auto& leftScope = streamFromLeftSide
            ? streamScope
            : lookupSourceScope;

        const auto& rightScope = !streamFromLeftSide
            ? streamScope
            : lookupSourceScope;

        auto* joinResultStructType = BuildJoinResultStructType(
            leftScope, rightScope, joinKind, columns, input->Pos(), ctx);

        if (!joinResultStructType) {
            return TStatus::Error;
        }

        input->SetTypeAnn(ctx.MakeType<TStreamExprType>(joinResultStructType));

        return TStatus::Ok;
    }

    TStatus HandleChunkedForwardList(const TExprNode::TPtr& input, TExprContext& ctx) {
        auto* stream = input->Child(TYtflowChunkedForwardList::idx_Stream);
        if (!EnsureStreamType(*stream, ctx)) {
            return TStatus::Error;
        }

        auto* itemType = stream->GetTypeAnn()
            ->Cast<TStreamExprType>()->GetItemType();

        input->SetTypeAnn(ctx.MakeType<TStreamExprType>(
            ctx.MakeType<TListExprType>(itemType)));

        return TStatus::Ok;
    }

    TStatus HandleHoppingAggregate(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 11, ctx)) {
            return TStatus::Error;
        }

        TVector<const TTypeAnnotationNode*> sourceItemTypes;
        TVector<const TTypeAnnotationNode*> sinkItemTypes;

        ui32 maxSinkOutputIndex;
        if (auto status = ValidateOpBase(
            input, ctx, sourceItemTypes, sinkItemTypes, maxSinkOutputIndex);

            status != TStatus::Ok
        ) {
            return status;
        }

        if (sourceItemTypes.size() != 1) {
            ctx.AddError(TIssue(
                ctx.GetPosition(input->Pos()),
                TStringBuilder()
                    << TYtflowHoppingAggregate::CallableName()
                    << " doesn't support multiple inputs"));

            return TStatus::Error;
        }

        auto* sourceStructType = sourceItemTypes[0]
            ->Cast<TStructExprType>();

        if (sinkItemTypes.size() != 1) {
            ctx.AddError(TIssue(
                ctx.GetPosition(input->Pos()),
                TStringBuilder()
                    << TYtflowHoppingAggregate::CallableName()
                    << " doesn't support multiple outputs"));

            return TStatus::Error;
        }

        auto* sinkStructType = sinkItemTypes[0]
            ->Cast<TStructExprType>();

        auto* keysNode = input->Child(TYtflowHoppingAggregate::idx_Keys);
        if (!EnsureTupleOfAtoms(*keysNode, ctx)) {
            return TStatus::Error;
        }

        auto keys = ParseTupleOfAtoms(*keysNode);

        if (keys.empty()) {
            ctx.AddError(TIssue(
                ctx.GetPosition(input->Pos()),
                TStringBuilder()
                    << "Whole stream hopping aggregate is not supported yet"));

            return TStatus::Error;
        }

        TVector<TString> unknownKeys;
        for (const auto& key : keys) {
            if (!sourceStructType->FindItem(key)) {
                unknownKeys.push_back(key);
            }
        }

        if (!unknownKeys.empty()) {
            ctx.AddError(TIssue(
                ctx.GetPosition(input->Pos()),
                TStringBuilder()
                    << "Unknown key columns: "
                    << JoinSeq(", ", unknownKeys)));

            return TStatus::Error;
        }

        auto* hopNode = input->Child(TYtflowHoppingAggregate::idx_Hop);
        if (!EnsureAtom(*hopNode, ctx)) {
            return TStatus::Error;
        }

        i64 hop;
        if (!TryFromString(hopNode->Content(), hop)) {
            ctx.AddError(TIssue(
                ctx.GetPosition(input->Pos()),
                TStringBuilder()
                    << "Invalid hop: "
                    << hopNode->Content()));

            return TStatus::Error;
        }

        if (hop <= 0) {
            ctx.AddError(TIssue(
                ctx.GetPosition(input->Pos()),
                TStringBuilder()
                    << "Expected positive hop, but got: "
                    << hop));

            return TStatus::Error;
        }

        auto* intervalNode = input->Child(TYtflowHoppingAggregate::idx_Interval);
        if (!EnsureAtom(*intervalNode, ctx)) {
            return TStatus::Error;
        }

        i64 interval;
        if (!TryFromString(intervalNode->Content(), interval)) {
            ctx.AddError(TIssue(
                ctx.GetPosition(input->Pos()),
                TStringBuilder()
                    << "Invalid interval: "
                    << intervalNode->Content()));

            return TStatus::Error;
        }

        if (interval <= 0) {
            ctx.AddError(TIssue(
                ctx.GetPosition(input->Pos()),
                TStringBuilder()
                    << "Expected positive interval, but got: "
                    << interval));

            return TStatus::Error;
        }

        if (interval % hop) {
            ctx.AddError(TIssue(
                ctx.GetPosition(input->Pos()),
                TStringBuilder()
                    << "Expected interval to be divisible by hop"
                    << ", interval -> " << interval
                    << ", hop -> " << hop));

            return TStatus::Error;
        }

        auto* delayNode = input->Child(TYtflowHoppingAggregate::idx_Delay);
        if (!EnsureAtom(*delayNode, ctx)) {
            return TStatus::Error;
        }

        i64 delay;
        if (!TryFromString(delayNode->Content(), delay)) {
            ctx.AddError(TIssue(
                ctx.GetPosition(input->Pos()),
                TStringBuilder()
                    << "Invalid delay: "
                    << delayNode->Content()));

            return TStatus::Error;
        }

        if (delay < 0) {
            ctx.AddError(TIssue(
                ctx.GetPosition(input->Pos()),
                TStringBuilder()
                    << "Expected non-negative delay, but got: "
                    << delay));

            return TStatus::Error;
        }

        auto* savedStateTypeNode = input->Child(TYtflowHoppingAggregate::idx_SavedStateType);
        if (!EnsureType(*savedStateTypeNode, ctx)) {
            return TStatus::Error;
        }

        auto* savedStateType = savedStateTypeNode->GetTypeAnn()
            ->Cast<TTypeExprType>()->GetType();

        if (!EnsureListType(savedStateTypeNode->Pos(), *savedStateType, ctx)) {
            return TStatus::Error;
        }

        auto* savedStateItemType = savedStateType
            ->Cast<TListExprType>()->GetItemType();

        if (!EnsureTupleTypeSize(savedStateTypeNode->Pos(), savedStateItemType, 2, ctx)) {
            return TStatus::Error;
        }

        auto* savedStateTupleType = savedStateItemType->Cast<TTupleExprType>();

        auto* columnType = savedStateTupleType->GetItems()[0];

        auto& updateStateLambda = input->ChildRef(TYtflowHoppingAggregate::idx_UpdateStateLambda);
        if (auto status = ConvertToLambda(updateStateLambda, ctx, 2, 2);
            status != TStatus::Ok
        ) {
            return status;
        }

        auto updateStateLambdaArgTypes = std::vector<const TTypeAnnotationNode*>{
            ctx.MakeType<TStreamExprType>(sourceStructType),
            savedStateType
        };

        if (!UpdateLambdaAllArgumentsTypes(
            updateStateLambda,
            updateStateLambdaArgTypes,
            ctx
        )) {
            return TStatus::Error;
        }

        if (!updateStateLambda->GetTypeAnn()) {
            return TStatus::Repeat;
        }

        auto timerTimestampsTupleItems = std::vector<const TTypeAnnotationNode*>{
            ctx.MakeType<TDataExprType>(EDataSlot::Timestamp),
            ctx.MakeType<TDataExprType>(EDataSlot::Timestamp)
        };

        auto expectedUpdateStateTupleItems = std::vector<const TTypeAnnotationNode*>{
            savedStateType,
            ctx.MakeType<TListExprType>(
                ctx.MakeType<TTupleExprType>(std::move(timerTimestampsTupleItems)))
            };

        auto expectedUpdateStateLambdaType = ctx.MakeType<TTupleExprType>(
            std::move(expectedUpdateStateTupleItems));

        if (updateStateLambda->GetTypeAnn() != expectedUpdateStateLambdaType) {
            ctx.AddError(TIssue(
                ctx.GetPosition(input->Pos()),
                TStringBuilder()
                    << "Unexpected update state lambda output type: "
                    << ToString(*updateStateLambda->GetTypeAnn()) << " (got) != "
                    << ToString(*savedStateType) << " (expected)"));

            return TStatus::Error;
        }

        auto& postprocessLambda = input->ChildRef(TYtflowHoppingAggregate::idx_PostprocessLambda);
        if (auto status = ConvertToLambda(postprocessLambda, ctx, 3, 3);
            status != TStatus::Ok
        ) {
            return status;
        }

        std::vector<const TTypeAnnotationNode*> keyTypes;
        for (const auto& key : keys) {
            keyTypes.push_back(sourceStructType->FindItemType(key));
        }

        const TTypeAnnotationNode* keysType = nullptr;
        if (keyTypes.size() == 1) {
            keysType = keyTypes[0];
        } else {
            keysType = ctx.MakeType<TTupleExprType>(keyTypes);
        }

        if (!UpdateLambdaAllArgumentsTypes(
            postprocessLambda,
            {keysType, savedStateType, columnType},
            ctx
        )) {
            return TStatus::Error;
        }

        if (!postprocessLambda->GetTypeAnn()) {
            return TStatus::Repeat;
        }

        auto postprocessType = postprocessLambda->GetTypeAnn();

        if (!EnsureTupleTypeSize(input->Pos(), postprocessType, 3, ctx)) {
            return TStatus::Error;
        }

        auto* postprocessTupleType = postprocessType->Cast<TTupleExprType>();

        auto* postprocessedFramesType = postprocessTupleType->GetItems()[0];
        auto* remainingFramesType = postprocessTupleType->GetItems()[1];
        auto* cleanupStateFlagType = postprocessTupleType->GetItems()[2];

        if (!EnsureStreamType(input->Pos(), *postprocessedFramesType, ctx)) {
            return TStatus::Error;
        }

        if (remainingFramesType != savedStateType) {
            ctx.AddError(TIssue(
                ctx.GetPosition(input->Pos()),
                TStringBuilder()
                    << "Unexpected postprocess lambda remaining frames type: "
                    << ToString(*remainingFramesType) << " (got) != "
                    << ToString(*savedStateType) << " (expected)"));

            return TStatus::Error;
        }

        if (!EnsureSpecificDataType(
            input->Pos(),
            *cleanupStateFlagType,
            EDataSlot::Bool,
            ctx
        )) {
            return TStatus::Error;
        }

        auto* postprocessedFramesItemType = postprocessedFramesType
            ->Cast<TStreamExprType>()->GetItemType();

        if (!EnsureStructType(input->Pos(), *postprocessedFramesItemType, ctx)) {
            return TStatus::Error;
        }

        auto* postprocessedFramesStructType = postprocessedFramesItemType
            ->Cast<TStructExprType>();

        const auto& sinksNode = input->Child(TYtflowOpBase::idx_Sinks);
        auto sink = sinksNode->ChildPtr(0);

        if (postprocessedFramesStructType != sinkStructType) {
            ctx.AddError(TIssue(
                ctx.GetPosition(input->Pos()),
                TStringBuilder()
                    << "Expected type: " << *static_cast<const TTypeAnnotationNode*>(sinkStructType)
                    << ", but got: " << *static_cast<const TTypeAnnotationNode*>(postprocessedFramesItemType)));

            return TStatus::Error;
        }

        input->SetTypeAnn(ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            input->Child(TYtflowOpBase::idx_World)->GetTypeAnn(),
            ctx.MakeType<TListExprType>(sinkStructType)
        }));

        // TODO(ngc224): find out why input requires this restriction.
        // Without this the following error occurs: Rewrite error, missing Multi(0:{},1:{}) constraint in node YtflowHoppingAggregate!
        input->AddConstraint(ctx.MakeConstraint<TMultiConstraintNode>(
            NSorted::TSimpleMap<ui32, TConstraintSet>{
                { 0, {} },
                { 1, {} }
            }
        ));

        return TStatus::Ok;
    }

private:
    TYtflowState::TPtr State_;
};


THolder<TVisitorTransformerBase> CreateYtflowDataSinkTypeAnnotationTransformer(TYtflowState::TPtr state) {
    return MakeHolder<TYtflowDataSinkTypeAnnotationTransformer>(std::move(state));
}

} // namespace NYql
