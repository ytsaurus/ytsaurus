#pragma once

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_type_annotation.h>

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>

#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <utility>


namespace google::protobuf {

class Any;

} // namespace google::protobuf

namespace NYql {

IDataProvider* GetDataProvider(
    const TExprNode& node,
    const TTypeAnnotationContext& typeCtx);

std::pair<TString, IYtflowIntegration*> GetYtflowIntegrationWithProviderName(
    const TExprNode& node,
    const TTypeAnnotationContext& typeCtx);

IYtflowIntegration* GetYtflowIntegration(
    const TExprNode& node,
    const TTypeAnnotationContext& typeCtx);

IYtflowOptimization* GetYtflowOptimization(
    const TExprNode& node,
    const TTypeAnnotationContext& typeCtx);

bool EnsureSpecificCallable(
    const TExprNode& node,
    const THashSet<TStringBuf>& callableNames,
    TExprContext& ctx);

bool EnsureSpecificDataSource(
    const TExprNode& node,
    const THashSet<TStringBuf>& expectedCategories,
    TExprContext& ctx);

bool EnsureSpecificDataSink(
    const TExprNode& node,
    const THashSet<TStringBuf>& expectedCategories,
    TExprContext& ctx);

bool EnsureYtflowOutputItemType(
    TPositionHandle position,
    const TTypeAnnotationNode& type,
    TExprContext& ctx);

bool EnsureListOfStructs(
    TPositionHandle position,
    const TTypeAnnotationNode& type,
    TExprContext& ctx);

bool IsYtPersistentSink(
    const TExprNode& node,
    TExprContext& ctx,
    const TTypeAnnotationContext& typeCtx);

bool TryGetYtSinkSettings(
    const TExprNode& node,
    TExprContext& ctx,
    const TTypeAnnotationContext& typeCtx,
    ::google::protobuf::Any& settings);

bool IsYtflowProviderInput(const TExprNode& node);

bool IsSingleOutput(const NNodes::TYtflowMapBase& map);

TExprNode::TPtr RemoveSettings(
    const TExprNode& settings,
    const THashSet<TStringBuf>& settingNames,
    TExprContext& ctx);

using TYtflowConsumer = std::pair<const TExprNode*, NNodes::TYtflowOutput>;

TMaybe<TVector<TYtflowConsumer>> GetConsumersUnordered(
    const NNodes::TYtflowOpBase& operation,
    const TParentsMap& parentsMap);

TVector<TYtflowConsumer> GetConsumersUnorderedStrict(
    const NNodes::TYtflowOpBase& operation,
    const TParentsMap& parentsMap);

// Must be used only after CSEE or UpdateCompletness.
TMaybe<TVector<TYtflowConsumer>> GetConsumersOrdered(
    const NNodes::TYtflowOpBase& operation,
    const TParentsMap& parentsMap);

// Must be used only after CSEE or UpdateCompletness.
TVector<TYtflowConsumer> GetConsumersOrderedStrict(
    const NNodes::TYtflowOpBase& operation,
    const TParentsMap& parentsMap);

TVector<NNodes::TCoLeft> GetLeftNodesFromParents(
    const NNodes::TYtflowOpBase& operation,
    const TParentsMap& parentsMap);

TExprNode::TPtr BuildOperationSource(
    const TExprNode::TPtr& input,
    TSyncMap& syncList,
    TExprContext& ctx,
    const TTypeAnnotationContext& typeCtx);

const TStructExprType* FilterMembers(
    const TStructExprType* structType,
    const TVector<TStringBuf>& members,
    TExprContext& ctx);

TVector<TString> ParseTupleOfAtoms(const TExprNode& node);

bool IsTrivialLambda(const TExprNode& node);

} // namespace NYql
