#pragma once

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_type_annotation.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <utility>


namespace NYql {

namespace NYtflow::NProto {

class TQYTSinkMessage;

} // namespace NYtflow::NProto

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

bool IsYtPersistentSink(
    const TExprNode& node,
    TExprContext& ctx,
    const TTypeAnnotationContext& typeCtx);

bool TryGetYtSinkSettings(
    const TExprNode& node,
    TExprContext& ctx,
    const TTypeAnnotationContext& typeCtx,
    NYtflow::NProto::TQYTSinkMessage& settings);

bool IsYtflowProviderInput(const TExprNode& node);

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
