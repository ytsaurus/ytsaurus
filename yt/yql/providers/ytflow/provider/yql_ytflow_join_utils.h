#pragma once

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_join.h>
#include <yql/essentials/core/yql_type_annotation.h>

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>
#include <yt/yql/providers/ytflow/integration/mkql_interface/yql_ytflow_lookup_provider.h>

#include <util/generic/hash.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>


namespace NYql {

struct TCollectEquiJoinInputsByLabelResult {
    THashMap<TStringBuf, TExprNode::TPtr> StreamInputsByLabel;
    THashMap<TStringBuf, TExprNode::TPtr> LookupSourceInputsByLabel;
    THashMap<TStringBuf, TVector<const TItemExprType*>> ColumnTypes;

    bool IsYtflowProviderBoundEquiJoin = true;
    bool HasErrors = false;
};

struct TCollectEquiJoinInputsInfoByLabelResult {
    THashMap<TStringBuf, TVector<TStringBuf>> JoinKeyColumnsByLabel;
    THashMap<TStringBuf, ERowSelectionMode> RowSelectionModeByLabel;
};

struct TLookupJoinScope {
    TStringBuf Label;
    TVector<const TItemExprType*> ColumnTypes;
    const TStructExprType* InputType;
    bool IsLeftSide;
    TVector<TStringBuf> Keys;
    ERowSelectionMode RowSelectionMode;
};


TExprNode::TPtr MakeSyncNodeFromSyncList(
    const TSyncMap& syncList,
    TPositionHandle position,
    TExprContext& ctx);

TCollectEquiJoinInputsByLabelResult CollectEquiJoinInputsByLabel(
    const TExprNode& joinTree,
    const THashMap<TStringBuf, TVector<TStringBuf>>& joinKeyColumnsByLabel,
    const THashMap<TStringBuf, ERowSelectionMode>& rowSelectionModeByLabel,
    TExprContext& ctx,
    const TTypeAnnotationContext& typeCtx);

TCollectEquiJoinInputsInfoByLabelResult CollectEquiJoinInputsInfoByLabel(
    const TExprNode& joinTuple);

TExprNode::TPtr BuildJoinNodeFromEquiJoinTuple(
    const TExprNode& joinTuple,
    const THashMap<TStringBuf, TVector<const TItemExprType*>>& columnsByLabel,
    const THashMap<TStringBuf, TExprNode::TPtr>& streamInputsByLabel,
    const THashMap<TStringBuf, TExprNode::TPtr>& lookupSourceInputsByLabel,
    const THashMap<TStringBuf, ERowSelectionMode>& rowSelectionModeByLabel,
    TPositionHandle position,
    TExprContext& ctx,
    const TTypeAnnotationContext& typeCtx);

bool ValidateLookupJoinKind(
    TStringBuf joinKind,
    bool streamFromLeftSide,
    TPositionHandle position,
    TExprContext& ctx);

bool ValidateLookupJoinScope(
    const TExprNode& input,
    const TStructExprType* itemType,
    TLookupJoinScope& lookupJoinScope,
    TExprContext& ctx);

bool ValidateLookupJoinScopes(
    const TExprNode& streamScopeNode,
    const TStructExprType* streamStructType,
    const TExprNode& lookupSourceScopeNode,
    const TStructExprType* lookupSourceStructType,
    TPositionHandle position,
    TLookupJoinScope& streamScope,
    TLookupJoinScope& lookupSourceScope,
    TExprContext& ctx);

bool ValidateLookupJoinKeys(
    const TLookupJoinScope& streamScope,
    const TLookupJoinScope& lookupSourceScope,
    TPositionHandle position,
    TExprContext& ctx);

bool ValidateJoinLinkSettings(
    const TExprNode& joinLinkSettings,
    TPositionHandle position,
    TExprContext& ctx);

const TStructExprType* BuildJoinResultStructType(
    const TLookupJoinScope& leftLabel,
    const TLookupJoinScope& rightLabel,
    TStringBuf joinKind,
    const TVector<TString>& columns,
    TPositionHandle position,
    TExprContext& ctx);

} // namespace NYql
