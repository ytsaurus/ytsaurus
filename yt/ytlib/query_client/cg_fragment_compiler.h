#pragma once

#include "cg_helpers.h"
#include "cg_ir_builder.h"
#include "cg_types.h"
#include "query_common.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

typedef std::function<void(TCGOperatorContext& builder)> TCodegenSource;

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

DEFINE_ENUM(EStreamTag,
    (Final)
    (Intermediate)
    (Totals)
);

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
    TString name);

TCodegenValue MakeCodegenFunctionContext(
    int index);

TCodegenExpression MakeCodegenFunctionExpr(
    TString functionName,
    std::vector<TCodegenExpression> codegenArgs,
    EValueType type,
    TString name,
    const IFunctionRegistryPtr functionRegistry);

TCodegenExpression MakeCodegenUnaryOpExpr(
    EUnaryOp opcode,
    TCodegenExpression codegenOperand,
    EValueType type,
    TString name);

TCodegenExpression MakeCodegenBinaryOpExpr(
    EBinaryOp opcode,
    TCodegenExpression codegenLhs,
    TCodegenExpression codegenRhs,
    EValueType type,
    TString name);

TCodegenExpression MakeCodegenInOpExpr(
    std::vector<TCodegenExpression> codegenArgs,
    int arrayIndex);

////////////////////////////////////////////////////////////////////////////////

void CodegenEmptyOp(TCGOperatorContext& builder);

std::tuple<size_t, size_t> MakeCodegenSplitOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t slot);

size_t MakeCodegenMergeOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t firstSlot,
    size_t secondSlot);

size_t MakeCodegenScanOp(
    TCodegenSource* codegenSource,
    size_t* slotIndex);

std::tuple<size_t, size_t, size_t> MakeCodegenSplitterOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t slot,
    size_t streamIndex);

size_t MakeCodegenFilterOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t slot,
    TCodegenExpression codegenPredicate);

size_t MakeCodegenJoinOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t slot,
    int index,
    std::vector<std::pair<TCodegenExpression, bool>> equations,
    size_t commonKeyPrefix);

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
    int keySize);

size_t MakeCodegenGroupOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t slot,
    std::function<void(TCGContext&, Value*)> codegenInitialize,
    std::function<void(TCGContext&, Value*, Value*)> codegenEvaluateGroups,
    std::function<void(TCGContext&, Value*, Value*)> codegenEvaluateAggregateArgs,
    std::function<void(TCGContext&, Value*, Value*)> codegenUpdate,
    std::vector<EValueType> keyTypes,
    bool isMerge,
    int groupRowSize,
    bool checkNulls);

size_t MakeCodegenFinalizeOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t slot,
    std::function<void(TCGContext&, Value*)> codegenFinalize);

size_t MakeCodegenAddStreamOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t slot,
    std::vector<EValueType> sourceSchema,
    EStreamTag value);

size_t MakeCodegenOrderOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t slot,
    std::vector<TCodegenExpression> codegenExprs,
    std::vector<EValueType> orderColumnTypes,
    std::vector<EValueType> sourceSchema,
    const std::vector<bool>& isDesc);

size_t MakeCodegenProjectOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t slot,
    std::vector<TCodegenExpression> codegenArgs);

void MakeCodegenWriteOp(
    TCodegenSource* codegenSource,
    size_t slot);

////////////////////////////////////////////////////////////////////////////////

TCGQueryCallback CodegenEvaluate(
    const TCodegenSource* codegenSource,
    size_t slotIndex,
    size_t opaqueValuesCount);

TCGExpressionCallback CodegenExpression(TCodegenExpression codegenExpression, size_t opaqueValuesCount);

TCGAggregateCallbacks CodegenAggregate(TCodegenAggregate codegenAggregate);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

