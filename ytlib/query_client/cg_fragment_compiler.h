#pragma once

#include "cg_helpers.h"
#include "cg_ir_builder.h"
#include "cg_types.h"
#include "query_common.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

typedef std::function<void(TCGContext& builder, Value* row)> TCodegenConsumer;
typedef std::function<void(TCGOperatorContext& builder, const TCodegenConsumer& codegenConsumer)> TCodegenSource;

typedef std::function<Value*(TCGIRBuilderPtr& builder)> TCodegenBlock;
typedef std::function<Value*(TCGBaseContext& builder)> TCodegenValue;
typedef std::function<TCGValue(TCGExprContext& builder, Value* row)> TCodegenExpression;

typedef std::function<TCGValue(TCGExprContext& builder, Value* aggState)> TCodegenAggregateInit;
typedef std::function<TCGValue(TCGExprContext& builder, Value* aggState, Value* newValue)> TCodegenAggregateUpdate;
typedef std::function<TCGValue(TCGExprContext& builder, Value* dstAggState, Value* aggState)> TCodegenAggregateMerge;
typedef std::function<TCGValue(TCGExprContext& builder, Value* aggState)> TCodegenAggregateFinalize;

struct TCodegenAggregate {
    TCodegenAggregateInit Initialize;
    TCodegenAggregateUpdate Update;
    TCodegenAggregateMerge Merge;
    TCodegenAggregateFinalize Finalize;
};

////////////////////////////////////////////////////////////////////////////////

Value* CodegenLexicographicalCompare(
    TCGContext& builder,
    Value* lhsData,
    Value* lhsLength,
    Value* rhsData,
    Value* rhsLength);

TCodegenExpression MakeCodegenLiteralExpr(
    int index,
    EValueType type);

TCodegenExpression MakeCodegenReferenceExpr(
    int index,
    EValueType type,
    Stroka name);

TCodegenValue MakeCodegenFunctionContext(
    int index);

TCodegenExpression MakeCodegenFunctionExpr(
    Stroka functionName,
    std::vector<TCodegenExpression> codegenArgs,
    EValueType type,
    Stroka name,
    const IFunctionRegistryPtr functionRegistry);

TCodegenExpression MakeCodegenUnaryOpExpr(
    EUnaryOp opcode,
    TCodegenExpression codegenOperand,
    EValueType type,
    Stroka name);

TCodegenExpression MakeCodegenBinaryOpExpr(
    EBinaryOp opcode,
    TCodegenExpression codegenLhs,
    TCodegenExpression codegenRhs,
    EValueType type,
    Stroka name);

TCodegenExpression MakeCodegenInOpExpr(
    std::vector<TCodegenExpression> codegenArgs,
    int arrayIndex);

////////////////////////////////////////////////////////////////////////////////

void CodegenScanOp(
    TCGOperatorContext& builder,
    const TCodegenConsumer& codegenConsumer);

TCodegenSource MakeCodegenFilterOp(
    TCodegenExpression codegenPredicate,
    TCodegenSource codegenSource);

TCodegenSource MakeCodegenJoinOp(
    int index,
    std::vector<std::pair<TCodegenExpression, bool>> equations,
    size_t commonKeyPrefix,
    TCodegenSource codegenSource);

std::function<void(TCGContext&, Value*, Value*)> MakeCodegenEvaluateGroups(
    std::vector<TCodegenExpression> codegenGroupExprs,
    std::vector<EValueType> nullTypes = std::vector<EValueType>());

std::function<void(TCGContext&, Value*, Value*)> MakeCodegenEvaluateAggregateArgs(
    size_t keySize,
    std::vector<TCodegenExpression> codegenAggregateExprs);

std::function<void(TCGContext& builder, Value* row)> MakeCodegenAggregateInitialize(
    std::vector<TCodegenAggregate> codegenAggregates,
    int keySize);

std::function<void(TCGContext& builder, Value*, Value*)> MakeCodegenAggregateUpdate(
    std::vector<TCodegenAggregate> codegenAggregates,
    int keySize,
    bool isMerge);

std::function<void(TCGContext& builder, Value* row)> MakeCodegenAggregateFinalize(
    std::vector<TCodegenAggregate> codegenAggregates,
    int keySize,
    bool isFinal);

TCodegenSource MakeCodegenGroupOp(
    std::function<void(TCGContext&, Value*)> codegenInitialize,
    std::function<void(TCGContext&, Value*, Value*)> codegenEvaluateGroups,
    std::function<void(TCGContext&, Value*, Value*)> codegenEvaluateAggregateArgs,
    std::function<void(TCGContext&, Value*, Value*)> codegenUpdate,
    std::function<void(TCGContext&, Value*)> codegenFinalize,
    TCodegenSource codegenSource,
    std::vector<EValueType> keyTypes,
    bool isMerge,
    int groupRowSize,
    bool appendToSource,
    bool checkNulls);

TCodegenSource MakeCodegenOrderOp(
    std::vector<TCodegenExpression> codegenExprs,
    std::vector<EValueType> sourceSchema,
    TCodegenSource codegenSource,
    const std::vector<bool>& isDesc);

TCodegenSource MakeCodegenProjectOp(
    std::vector<TCodegenExpression> codegenArgs,
    TCodegenSource codegenSource);

////////////////////////////////////////////////////////////////////////////////

TCGQueryCallback CodegenEvaluate(TCodegenSource codegenSource, size_t opaqueValuesCount);

TCGExpressionCallback CodegenExpression(TCodegenExpression codegenExpression, size_t opaqueValuesCount);

TCGAggregateCallbacks CodegenAggregate(TCodegenAggregate codegenAggregate);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

