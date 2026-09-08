#include "yql_ytflow_join_utils.h"
#include "yql_ytflow_utils.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>
#include <yt/yql/providers/ytflow/integration/interface/yql_ytflow_integration.h>

#include <util/string/join.h>


namespace {

using namespace NYql;
using namespace NNodes;

struct TJoinInput {
    TStringBuf Label;
    TVector<const TItemExprType*> ColumnTypes;
    TExprNode::TPtr Input;
    ERowSelectionMode RowSelectionMode;
    bool CanLookupRead;
};

void FillJoinKeys(
    const TExprNode& keysNode,
    THashMap<TStringBuf, TVector<TStringBuf>>& tableKeysMap
) {
    auto keys = TExprBase(&keysNode).Cast<TCoAtomList>();
    YQL_ENSURE(keys.Size() % 2 == 0, "Unexpected keys container size " << keys.Size()
        << ", values: " << JoinSeq(", ", TVector<TStringBuf>(keys.begin(), keys.end())));
    for (ui32 index = 0; index < keys.Size(); index += 2) {
        auto table = keys.Item(index).Value();
        auto key = keys.Item(index + 1).Value();
        tableKeysMap[table].push_back(key);
    }
}

TVector<TString> FillJoinKeys(
    const TVector<TStringBuf>& keys,
    bool addLabel
) {
    YQL_ENSURE(keys.size() % 2 == 0, "Unexpected keys container size " << keys.size()
        << ", values: " << JoinSeq(", ", keys));
    TVector<TString> result;
    for (ui32 index = 0; index < keys.size(); index += 2) {
        TStringBuf table = keys[index];
        TStringBuf key = keys[index + 1];
        TString resultKey = addLabel
            ? TString::Join(table, '.', key)
            : TString(key);
        result.push_back(std::move(resultKey));
    }
    return result;
}

void AddOutputColumnTypes(
    const TVector<const TItemExprType*>& columnTypes,
    bool isSideOptional,
    TStringBuf label,
    TVector<const TItemExprType*>& items,
    TExprContext& ctx
) {
    for (auto* columnType : columnTypes) {
        auto* augmentedMemberType = columnType->GetItemType();

        if (isSideOptional && !augmentedMemberType->IsOptionalOrNull()) {
            augmentedMemberType = ctx.MakeType<TOptionalExprType>(
                augmentedMemberType);
        }

        TString resultName = label.empty()
            ? TString(columnType->GetName())
            : TString::Join(label, '.', columnType->GetName());

        items.push_back(
            ctx.MakeType<TItemExprType>(resultName, augmentedMemberType));
    }
}

void CollectEquiJoinInputsInfoByLabelImpl(
    const TCoEquiJoinTuple& equiJoinTuple,
    TCollectEquiJoinInputsInfoByLabelResult& result
) {
    auto fillRowSelectionModeByLabel = [&](auto& hints, const auto& label) {
        if (auto iterator = hints.find("any");
            iterator != hints.end()
        ) {
            result.RowSelectionModeByLabel.emplace(label, ERowSelectionMode::Any);
        } else {
            result.RowSelectionModeByLabel.emplace(label, ERowSelectionMode::All);
        }
    };

    auto leftScope = equiJoinTuple.LeftScope();
    auto rightScope = equiJoinTuple.RightScope();

    auto joinLinkSettings = GetEquiJoinLinkSettings(equiJoinTuple.Options().Ref());
    if (auto maybeLabel = leftScope.Maybe<TCoAtom>()) {
        auto label = maybeLabel.Cast().Value();
        FillJoinKeys(equiJoinTuple.LeftKeys().Ref(), result.JoinKeyColumnsByLabel);
        fillRowSelectionModeByLabel(joinLinkSettings.LeftHints, label);
    } else {
        CollectEquiJoinInputsInfoByLabelImpl(leftScope.Cast<TCoEquiJoinTuple>(), result);
    }

    if (auto maybeLabel = rightScope.Maybe<TCoAtom>()) {
        auto label = maybeLabel.Cast().Value();
        FillJoinKeys(equiJoinTuple.RightKeys().Ref(), result.JoinKeyColumnsByLabel);
        fillRowSelectionModeByLabel(joinLinkSettings.RightHints, label);
    } else {
        CollectEquiJoinInputsInfoByLabelImpl(rightScope.Cast<TCoEquiJoinTuple>(), result);
    }
}

TMaybe<TJoinInput> BuildJoinNodeFromEquiJoinTupleImpl(
    const TExprNode& joinTuple,
    bool isSideOptional,
    const THashMap<TStringBuf, TVector<const TItemExprType*>>& columnsByLabel,
    const THashMap<TStringBuf, TExprNode::TPtr>& streamInputsByLabel,
    const THashMap<TStringBuf, TExprNode::TPtr>& lookupSourceInputsByLabel,
    const THashMap<TStringBuf, ERowSelectionMode>& rowSelectionModeByLabel,
    TPositionHandle position,
    TExprContext& ctx,
    const TTypeAnnotationContext& typeCtx
) {
    auto joinTreeNode = TExprBase(&joinTuple);
    if (auto maybeLabel = joinTreeNode.Maybe<TCoAtom>()) {
        auto label = maybeLabel.Cast().Value();
        auto [input, canLookupRead] = [&] {
            if (auto* streamInput = streamInputsByLabel.FindPtr(label)) {
                return std::pair(TCoEquiJoinInput(*streamInput).List().Ptr(), false);
            } else if (auto* lookupInput = lookupSourceInputsByLabel.FindPtr(label)) {
                return std::pair(TCoEquiJoinInput(*lookupInput).List().Ptr(), true);
            }
            YQL_ENSURE(false, "Unknown input with label: " << label);
        }();
        auto* rowSelectionMode = rowSelectionModeByLabel.FindPtr(label);
        YQL_ENSURE(rowSelectionMode, "Unknown row selection mode for label: " << label);

        TVector<const TItemExprType*> items;
        auto* columns = columnsByLabel.FindPtr(label);
        YQL_ENSURE(columns, "Unknown column types for label:" << label);
        AddOutputColumnTypes(*columns, isSideOptional, label, items, ctx);

        return TJoinInput{
            .Label = label,
            .ColumnTypes = std::move(items),
            .Input = std::move(input),
            .RowSelectionMode = *rowSelectionMode,
            .CanLookupRead = canLookupRead,
        };
    }

    auto joinTreeTuple = joinTreeNode.Cast<TCoEquiJoinTuple>();

    auto maybeLeftInput = BuildJoinNodeFromEquiJoinTupleImpl(
        joinTreeTuple.LeftScope().Ref(),
        IsLeftJoinSideOptional(joinTreeTuple.Type().Value()),
        columnsByLabel,
        streamInputsByLabel,
        lookupSourceInputsByLabel,
        rowSelectionModeByLabel,
        position,
        ctx,
        typeCtx);
    if (!maybeLeftInput) {
        return Nothing();
    }

    auto maybeRightInput = BuildJoinNodeFromEquiJoinTupleImpl(
        joinTreeTuple.RightScope().Ref(),
        IsRightJoinSideOptional(joinTreeTuple.Type().Value()),
        columnsByLabel,
        streamInputsByLabel,
        lookupSourceInputsByLabel,
        rowSelectionModeByLabel,
        position,
        ctx,
        typeCtx);
    if (!maybeRightInput) {
        return Nothing();
    }

    if (!ValidateJoinLinkSettings(joinTreeTuple.Options().Ref(), position, ctx)) {
        return Nothing();
    }

    auto& leftInput = *maybeLeftInput;
    auto& rightInput = *maybeRightInput;

    if (leftInput.CanLookupRead && rightInput.CanLookupRead) {
        ctx.AddError(TIssue(
            ctx.GetPosition(position),
            TStringBuilder()
                << "Join of two lookup sources"
                << " (with correlation names: " << leftInput.Label << ", " << rightInput.Label << ")"
                << " is not supported"));
        return Nothing();
    }

    bool streamFromLeftSide = !leftInput.CanLookupRead;

    auto joinKind = joinTreeTuple.Type().Value();
    if (!ValidateLookupJoinKind(joinKind, streamFromLeftSide, position, ctx)) {
        return Nothing();
    }

    auto leftKeysList = joinTreeTuple.LeftKeys().Cast<TCoAtomList>();
    auto rightKeysList = joinTreeTuple.RightKeys().Cast<TCoAtomList>();

    auto streamKeys = TVector<TStringBuf>(leftKeysList.begin(), leftKeysList.end());
    auto lookupSourceKeys = TVector<TStringBuf>(rightKeysList.begin(), rightKeysList.end());
    auto& streamInput = leftInput;
    auto& lookupSourceInput = rightInput;
    if (!streamFromLeftSide) {
        std::swap(streamInput, lookupSourceInput);
        std::swap(streamKeys, lookupSourceKeys);
    }

    if (streamInput.RowSelectionMode != ERowSelectionMode::All) {
        ctx.AddError(TIssue(
            ctx.GetPosition(position),
            TStringBuilder()
                << "Found unsupported stream side join modifier: "
                << ToString(streamInput.RowSelectionMode)));

        return Nothing();
    }

    auto joinItemTypes = std::move(leftInput.ColumnTypes);
    std::move(
        rightInput.ColumnTypes.begin(),
        rightInput.ColumnTypes.end(),
        std::back_inserter(joinItemTypes));

    auto joinResultType = ctx.MakeType<TStructExprType>(joinItemTypes);

    TVector<TString> streamKeyColumns = FillJoinKeys(
        streamKeys, streamInput.Label.empty());
    TVector<TString> lookupSourceKeyColumns = FillJoinKeys(
        lookupSourceKeys, lookupSourceInput.Label.empty());

    TSyncMap syncList;
    auto source = BuildOperationSource(
        streamInput.Input, syncList, ctx, typeCtx);

    auto lookupSourceProviderInput = TExprBase(lookupSourceInput.Input)
        .Cast<TYtflowReadWrap>().Input();
    auto* ytflowIntegration = GetYtflowIntegration(
        lookupSourceProviderInput.Ref(), typeCtx);

    YQL_ENSURE(ytflowIntegration);

    syncList.emplace(
        ytflowIntegration->GetReadWorld(lookupSourceProviderInput.Ref(), ctx),
        syncList.size());

    const ui32 outputIndex = 0;
    auto sink = Build<TYtflowIntermediateSink>(ctx, TPositionHandle{})
        .Name()
            .Value("")
            .Build()
        .OutputIndex()
            .Value(outputIndex)
            .Build()
        .RowType(ExpandType(TPositionHandle{}, *joinResultType, ctx))
        .Done();

    TVector<TString> columns;
    for (const auto& item : joinResultType->GetItems()) {
        columns.push_back(TString(item->GetName()));
    }

    auto lookupJoinLambda = Build<TCoLambda>(ctx, position)
        .Args({"stream"})
        .Body<TYtflowLookupJoin>()
            .Stream("stream")
            .LookupSource(std::move(lookupSourceInput.Input))
            .JoinKind()
                .Value(joinKind)
                .Build()
            .StreamScope()
                .Label()
                    .Value(streamInput.Label)
                    .Build()
                .Side()
                    .Value(streamFromLeftSide ? "left" : "right")
                    .Build()
                .Keys(
                    MakeAtomList(position, streamKeyColumns, ctx))
                .RowSelectionMode()
                    .Value(ToString(streamInput.RowSelectionMode))
                    .Build()
                .Build()
            .LookupSourceScope()
                .Label()
                    .Value(lookupSourceInput.Label)
                    .Build()
                .Side()
                    .Value(streamFromLeftSide ? "right" : "left")
                    .Build()
                .Keys(
                    MakeAtomList(
                        position, lookupSourceKeyColumns, ctx))
                .RowSelectionMode()
                    .Value(ToString(lookupSourceInput.RowSelectionMode))
                    .Build()
                .Build()
            .Columns(
                MakeAtomList(position, columns, ctx))
            .Build()
        .Done();

    auto map = Build<TYtflowTransformMap>(ctx, position)
        .World(MakeSyncNodeFromSyncList(syncList, position, ctx))
        .Sources()
            .Add(std::move(source))
            .Build()
        .Sinks()
            .Add(std::move(sink))
            .Build()
        .Settings()
            .Build()
        .Lambda(std::move(lookupJoinLambda))
        .GroupByColumns(
            MakeAtomList(position, streamKeyColumns, ctx))
        .Done();

    auto output = Build<TYtflowOutput>(ctx, position)
        .Operation(std::move(map))
        .OutputIndex()
            .Value(outputIndex)
            .Build()
        .Done();

    return TJoinInput{
        .ColumnTypes = std::move(joinItemTypes),
        .Input = output.Ptr(),
        .RowSelectionMode = ERowSelectionMode::All,
        .CanLookupRead = false,
    };
}

} // namespace

namespace NYql {

using namespace NNodes;

TExprNode::TPtr MakeSyncNodeFromSyncList(
    const TSyncMap& syncList,
    TPositionHandle position,
    TExprContext& ctx
) {
    using TPair = std::pair<TExprNode::TPtr, ui64>;
    TVector<TPair> sortedList(syncList.cbegin(), syncList.cend());
    TExprNode::TListType syncChildren;
    Sort(sortedList, [](const TPair& x, const TPair& y) { return x.second < y.second; });
    for (auto x : sortedList) {
        syncChildren.push_back(x.first);
    }

    return Build<TCoSync>(ctx, position)
        .Add(std::move(syncChildren))
        .Done().Ptr();
}

TCollectEquiJoinInputsByLabelResult CollectEquiJoinInputsByLabel(
    const TExprNode& joinTree,
    const THashMap<TStringBuf, TVector<TStringBuf>>& joinKeyColumnsByLabel,
    const THashMap<TStringBuf, ERowSelectionMode>& rowSelectionModeByLabel,
    TExprContext& ctx,
    const TTypeAnnotationContext& typeCtx
) {
    TCollectEquiJoinInputsByLabelResult result;

    auto equiJoin = TCoEquiJoin(&joinTree);

    for (size_t index = 0; index < equiJoin.ArgCount() - 2; ++index) {
        auto input = equiJoin.Arg(index).Cast<TCoEquiJoinInput>();
        auto list = input.List();
        auto label = input.Scope().Ref().Content();

        if (!IsYtflowProviderInput(list.Ref())) {
            result.IsYtflowProviderBoundEquiJoin = false;
            return result;
        }

        // cross join case
        if (joinKeyColumnsByLabel.find(label) == joinKeyColumnsByLabel.end()) {
            result.HasErrors = true;
            ctx.AddError(TIssue(
                ctx.GetPosition(list.Pos()),
                TStringBuilder()
                    << "Cross join is not supported, one of correlation names is " << label));
            return result;
        }

        YQL_ENSURE(
            rowSelectionModeByLabel.find(label) != rowSelectionModeByLabel.end(),
            "Label " << label << " is absent from row selection mode mapping");

        auto* itemType = list.Ref().GetTypeAnn()
            ->Cast<TListExprType>()->GetItemType()
            ->Cast<TStructExprType>();
        result.ColumnTypes[label] = itemType->GetItems();

        if (auto maybeReadWrap = list.Maybe<TYtflowReadWrap>()) {
            auto providerInput = maybeReadWrap.Cast().Input();
            auto* ytflowIntegration = GetYtflowIntegration(providerInput.Ref(), typeCtx);
            YQL_ENSURE(ytflowIntegration);

            auto canLookupRead = ytflowIntegration->CanLookupRead(
                providerInput.Ref(),
                joinKeyColumnsByLabel.at(label),
                rowSelectionModeByLabel.at(label),
                ctx);

            if (canLookupRead.Defined()) {
                if (!canLookupRead.GetRef()) {
                    result.HasErrors = true;
                } else {
                    result.LookupSourceInputsByLabel.emplace(label, input.Ptr());
                }

                continue;
            }
        }

        result.StreamInputsByLabel.emplace(label, input.Ptr());
    }

    return result;
}

TCollectEquiJoinInputsInfoByLabelResult CollectEquiJoinInputsInfoByLabel(const TExprNode& joinTuple) {
    auto equiJoinTuple = TCoEquiJoinTuple(&joinTuple);
    TCollectEquiJoinInputsInfoByLabelResult rowSelectionModeByLabel;
    CollectEquiJoinInputsInfoByLabelImpl(equiJoinTuple, rowSelectionModeByLabel);
    return rowSelectionModeByLabel;
}

TExprNode::TPtr BuildJoinNodeFromEquiJoinTuple(
    const TExprNode& joinTuple,
    const THashMap<TStringBuf, TVector<const TItemExprType*>>& columnsByLabel,
    const THashMap<TStringBuf, TExprNode::TPtr>& streamInputsByLabel,
    const THashMap<TStringBuf, TExprNode::TPtr>& lookupSourceInputsByLabel,
    const THashMap<TStringBuf, ERowSelectionMode>& rowSelectionModeByLabel,
    TPositionHandle position,
    TExprContext& ctx,
    const TTypeAnnotationContext& typeCtx
) {
    auto equiJoinTuple = TCoEquiJoinTuple(&joinTuple);
    auto result = BuildJoinNodeFromEquiJoinTupleImpl(
        joinTuple,
        false,
        columnsByLabel,
        streamInputsByLabel,
        lookupSourceInputsByLabel,
        rowSelectionModeByLabel,
        position,
        ctx,
        typeCtx);
    if (!result) {
        return {};
    }

    return result->Input;
}

bool ValidateJoinLinkSettings(
    const TExprNode& joinLinkSettings,
    TPositionHandle position,
    TExprContext& ctx
) {
    std::vector<TString> unsupportedSettings;
    auto checkSettings = [&](const auto& settings) {
        CopyIf(
            settings.begin(),
            settings.end(),
            std::back_inserter(unsupportedSettings),
            [](const auto& setting) { return !EqualToOneOf(setting, "any"); });
    };

    auto settings = GetEquiJoinLinkSettings(joinLinkSettings);
    checkSettings(settings.LeftHints);
    checkSettings(settings.RightHints);

    if (!unsupportedSettings.empty()) {
        ctx.AddError(TIssue(
            ctx.GetPosition(position),
            TStringBuilder()
                << "Found unsupported join modifiers: "
                << JoinSeq(", ", unsupportedSettings)));

        return false;
    }

    return true;
}

bool ValidateLookupJoinKind(
    TStringBuf joinKind,
    bool streamFromLeftSide,
    TPositionHandle position,
    TExprContext& ctx
) {
    bool supported = false;
    TStringBuf leftLabel, rightLabel;

    if (streamFromLeftSide) {
        supported = EqualToOneOf(joinKind, "Inner", "Left", "LeftOnly", "LeftSemi");
        leftLabel = "stream";
        rightLabel = "lookup source";
    } else {
        supported = EqualToOneOf(joinKind, "Inner", "Right", "RightOnly", "RightSemi");
        leftLabel = "lookup source";
        rightLabel = "stream";
    }

    if (!supported) {
        ctx.AddError(TIssue(
            ctx.GetPosition(position),
            TStringBuilder()
                << "Join of " << leftLabel << " and " << rightLabel
                << " of type " << joinKind << " is not supported yet"));

        return false;
    }

    return true;
}

bool ValidateLookupJoinScope(
    const TExprNode& input,
    const TStructExprType* itemType,
    TLookupJoinScope& lookupJoinScope,
    TExprContext& ctx
) {
    if (!EnsureArgsCount(input, 4, ctx)) {
        return false;
    }

    auto* label = input.Child(0);
    if (!EnsureAtom(*label, ctx)) {
        return false;
    }

    lookupJoinScope.Label = label->Content();

    auto* side = input.Child(1);
    if (!EnsureAtom(*side, ctx)) {
        return false;
    }

    auto allowedSides = THashSet<TStringBuf>{"left", "right"};

    if (!allowedSides.contains(side->Content())) {
        ctx.AddError(TIssue(
            ctx.GetPosition(side->Pos()),
            TStringBuilder()
                << "Unexpected side label: " << side->Content()));

        return false;
    }

    lookupJoinScope.IsLeftSide = side->Content() == "left";

    auto* keysNode = input.Child(2);
    if (!EnsureTupleOfAtoms(*keysNode, ctx)) {
        return false;
    }

    auto keys = ParseTupleOfAtoms(*keysNode);

    THashSet<TString> keyColumnsSet;
    TVector<TString> unknownKeyColumns;

    for (const auto& key : keys) {
        auto* keyType = itemType->FindItemType(key);
        if (!keyType) {
            unknownKeyColumns.push_back(key);
            continue;
        }

        if (!keyColumnsSet.insert(key).second) {
            ctx.AddError(TIssue(
                ctx.GetPosition(keysNode->Pos()),
                TStringBuilder()
                    << "Duplicate key column: " << key));

            return false;
        }

        lookupJoinScope.Keys.push_back(key);
    }

    if (!unknownKeyColumns.empty()) {
        ctx.AddError(TIssue(
            ctx.GetPosition(keysNode->Pos()),
            TStringBuilder()
                << "Unknown key columns: " << JoinSeq(", ", unknownKeyColumns)));

        return false;
    }

    auto* rowSelectionModeNode = input.Child(3);
    if (!EnsureAtom(*rowSelectionModeNode, ctx)) {
        return false;
    }

    ERowSelectionMode rowSelectionMode;
    if (!TryFromString<ERowSelectionMode>(
            rowSelectionModeNode->Content(), rowSelectionMode
    )) {
        ctx.AddError(TIssue(
            ctx.GetPosition(keysNode->Pos()),
            TStringBuilder()
                << "Unknown row selection mode: "
                << rowSelectionModeNode->Content()));

        return false;
    }

    lookupJoinScope.RowSelectionMode = rowSelectionMode;

    lookupJoinScope.InputType = itemType;

    return true;
}

bool ValidateLookupJoinScopes(
    const TExprNode& streamScopeNode,
    const TStructExprType* streamStructType,
    const TExprNode& lookupSourceScopeNode,
    const TStructExprType* lookupSourceStructType,
    TPositionHandle position,
    TLookupJoinScope& streamScope,
    TLookupJoinScope& lookupSourceScope,
    TExprContext& ctx
) {
    if (!ValidateLookupJoinScope(streamScopeNode, streamStructType, streamScope, ctx)) {
        return false;
    }

    if (!ValidateLookupJoinScope(
            lookupSourceScopeNode, lookupSourceStructType, lookupSourceScope, ctx
    )) {
        return false;
    }

    if (streamScope.Label && lookupSourceScope.Label && streamScope.Label == lookupSourceScope.Label) {
        ctx.AddError(TIssue(
            ctx.GetPosition(position),
            TStringBuilder()
                << "Duplicate correlation name: "
                << streamScope.Label));

        return false;
    }

    if (!(streamScope.IsLeftSide ^ lookupSourceScope.IsLeftSide)) {
        auto formatSideLabel = [](const auto& scope) {
            return scope.IsLeftSide
                ? "left"
                : "right";
        };

        ctx.AddError(TIssue(
            ctx.GetPosition(position),
            TStringBuilder()
                << "Unexpected side labels: "
                << formatSideLabel(streamScope) << ", "
                << formatSideLabel(lookupSourceScope)));

        return false;
    }

    if (streamScope.RowSelectionMode != ERowSelectionMode::All) {
        ctx.AddError(TIssue(
            ctx.GetPosition(position),
            TStringBuilder()
                << "Found unsupported stream side join modifier: "
                << ToString(streamScope.RowSelectionMode)));

        return false;
    }

    return true;
}

bool ValidateLookupJoinKeys(
    const TLookupJoinScope& streamScope,
    const TLookupJoinScope& lookupSourceScope,
    TPositionHandle position,
    TExprContext& ctx
) {
    auto* streamKeyTypes = FilterMembers(
        streamScope.InputType,
        streamScope.Keys,
        ctx);

    auto* lookupSourceKeyTypes = FilterMembers(
        lookupSourceScope.InputType,
        lookupSourceScope.Keys,
        ctx);

    if (streamKeyTypes != lookupSourceKeyTypes) {
        TStringBuf emptyLabel = "<empty label>";

        ctx.AddError(TIssue(
            ctx.GetPosition(position),
            TStringBuilder()
                << "Mismatch key column types: "
                << streamKeyTypes->ToString()
                << " (" << (streamScope.Label ? streamScope.Label : emptyLabel) << ") != "
                << lookupSourceKeyTypes->ToString()
                << " (" << (lookupSourceScope.Label ? lookupSourceScope.Label : emptyLabel) << ")"));

        return false;
    }

    return true;
}

const TStructExprType* BuildJoinResultStructType(
    const TLookupJoinScope& leftScope,
    const TLookupJoinScope& rightScope,
    TStringBuf joinKind,
    const TVector<TString>& columns,
    TPositionHandle position,
    TExprContext& ctx
) {
    // column -> visited
    TMap<TString, bool> columnInfo;
    for (const auto& column : columns) {
        columnInfo.emplace(column, false);
    }

    TVector<const TItemExprType*> items;

    bool isLeftSideOptional = IsLeftJoinSideOptional(joinKind);
    bool isRightSideOptional = IsRightJoinSideOptional(joinKind);

    AddOutputColumnTypes(
        leftScope.InputType->GetItems(), isLeftSideOptional, leftScope.Label, items, ctx);
    AddOutputColumnTypes(
        rightScope.InputType->GetItems(), isRightSideOptional, rightScope.Label, items, ctx);

    items.erase(std::remove_if(items.begin(), items.end(), [&](const auto* item) {
        if (auto iterator = columnInfo.find(item->GetName());
            iterator != columnInfo.end()
        ) {
            iterator->second = true;
            return false;
        }
        return true;
    }), items.end());

    TVector<TString> unknownColumns;
    for (const auto& [column, visited] : columnInfo) {
        if (!visited) {
            unknownColumns.push_back(column);
        }
    }

    if (!unknownColumns.empty()) {
        ctx.AddError(TIssue(
            ctx.GetPosition(position),
            TStringBuilder()
                << "Unknown join result columns: "
                << JoinSeq(", ", unknownColumns)));

        return nullptr;
    }

    return ctx.MakeType<TStructExprType>(std::move(items));
}

} // namespace NYql
