#pragma once

#include "cg_types.h"
#include "cg_ir_builder.h"
#include "cg_helpers.h"
#include "plan_fragment_common.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

typedef std::function<void(TCGContext& builder, Value* row)> TCodegenConsumer;
typedef std::function<void(TCGContext& builder, const TCodegenConsumer& codegenConsumer)> TCodegenSource;

typedef std::function<Value*(TCGIRBuilder& builder)> TCodegenBlock;
typedef std::function<Value*(TCGContext& builder)> TCodegenValue;
typedef std::function<TCGValue(TCGContext& builder, Value* row)> TCodegenExpression;

typedef std::function<TCGValue(TCGContext& builder, Value* aggState)> TCodegenAggregateInit;
typedef std::function<TCGValue(TCGContext& builder, Value* aggState, Value* newValue)> TCodegenAggregateUpdate;
typedef std::function<TCGValue(TCGContext& builder, Value* dstAggState, Value* aggState)> TCodegenAggregateMerge;
typedef std::function<TCGValue(TCGContext& builder, Value* aggState)> TCodegenAggregateFinalize;

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
    TCGContext& builder,
    const TCodegenConsumer& codegenConsumer);

TCodegenSource MakeCodegenFilterOp(
    TCodegenExpression codegenPredicate,
    TCodegenSource codegenSource);

TCodegenSource MakeCodegenJoinOp(
    int index,
    std::vector<TCodegenExpression> equations,
    TTableSchema sourceSchema,
    TCodegenSource codegenSource);

std::function<void(TCGContext&, Value*, Value*)> MakeCodegenEvaluateGroups(
    std::vector<TCodegenExpression> codegenGroupExprs);

std::function<void(TCGContext&, Value*, Value*)> MakeCodegenEvaluateAggregateArgs(
    std::vector<TCodegenExpression> codegenGroupExprs,
    std::vector<TCodegenExpression> codegenAggregateExprs,
    std::vector<TCodegenAggregate> codegenAggregates,
    bool isMerge,
    TTableSchema inputSchema);

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
    int groupRowSize);

TCodegenSource MakeCodegenOrderOp(
    std::vector<TCodegenExpression> codegenExprs,
    TTableSchema sourceSchema,
    TCodegenSource codegenSource,
    const std::vector<bool>& isDesc);

TCodegenSource MakeCodegenProjectOp(
    std::vector<TCodegenExpression> codegenArgs,
    TCodegenSource codegenSource);

////////////////////////////////////////////////////////////////////////////////

TCGQueryCallback CodegenEvaluate(
    TCodegenSource codegenSource);

TCGExpressionCallback CodegenExpression(
    TCodegenExpression codegenExpression);

TCGAggregateCallbacks CodegenAggregate(
    TCodegenAggregate codegenAggregate);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

