#pragma once

#include "cg_helpers.h"
#include "cg_ir_builder.h"
#include "cg_types.h"
#include "query_common.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

typedef std::function<void(TCGOperatorContext& builder)> TCodegenSource;
typedef std::function<TCGValue(TCGBaseContext& builder)> TCodegenValue;
typedef std::function<TCGValue(TCGExprContext& builder)> TCodegenExpression;

struct TCodegenFragmentInfo
{
    TCodegenFragmentInfo(TCodegenExpression generator, EValueType type, bool nullable, bool forceInline = false)
        : Generator(std::move(generator))
        , Type(type)
        , Nullable(nullable)
        , ForceInline(forceInline)
    { }

    TCodegenExpression Generator;
    EValueType Type;
    bool Nullable;
    bool ForceInline;
    size_t UseCount = 0;
    size_t Index = std::numeric_limits<size_t>::max();


    bool IsOutOfLine() const {
        return UseCount > 1 && !ForceInline;
    }

};

DECLARE_REFCOUNTED_STRUCT(TCodegenFragmentInfos)

struct TCodegenFragmentInfos
    : public TIntrinsicRefCounted
{
    std::vector<TCodegenFragmentInfo> Items;
    std::vector<Function*> Functions;
    TString NamePrefix;
};

DEFINE_REFCOUNTED_TYPE(TCodegenFragmentInfos)

typedef std::function<TCGValue(TCGBaseContext& builder, Value* buffer)> TCodegenAggregateInit;
typedef std::function<TCGValue(TCGBaseContext& builder, Value* buffer, TCGValue aggState, TCGValue newValue)> TCodegenAggregateUpdate;
typedef std::function<TCGValue(TCGBaseContext& builder, Value* buffer, TCGValue dstAggState, TCGValue aggState)> TCodegenAggregateMerge;
typedef std::function<TCGValue(TCGBaseContext& builder, Value* buffer, TCGValue aggState)> TCodegenAggregateFinalize;

struct TCodegenAggregate
{
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

DECLARE_REFCOUNTED_STRUCT(TComparerManager);

TComparerManagerPtr MakeComparerManager();

Value* CodegenLexicographicalCompare(
    TCGBaseContext& builder,
    Value* lhsData,
    Value* lhsLength,
    Value* rhsData,
    Value* rhsLength);

TCodegenExpression MakeCodegenLiteralExpr(
    int index,
    bool nullable,
    EValueType type);

TCodegenExpression MakeCodegenReferenceExpr(
    int index,
    EValueType type,
    TString name);

TCGValue CodegenFragment(
    TCGExprContext& builder,
    size_t id);

void MakeCodegenFragmentBodies(
    TCodegenSource* codegenSource,
    TCodegenFragmentInfosPtr fragmentInfos);

TCodegenExpression MakeCodegenUnaryOpExpr(
    EUnaryOp opcode,
    size_t operandId,
    EValueType type,
    TString name);

TCodegenExpression MakeCodegenBinaryOpExpr(
    EBinaryOp opcode,
    size_t lhsId,
    size_t rhsId,
    EValueType type,
    TString name);

TCodegenExpression MakeCodegenInExpr(
    std::vector<size_t> argIds,
    int arrayIndex,
    int hashtableIndex,
    TComparerManagerPtr comparerManager);

TCodegenExpression MakeCodegenBetweenExpr(
    std::vector<size_t> argIds,
    int arrayIndex,
    TComparerManagerPtr comparerManager);

TCodegenExpression MakeCodegenTransformExpr(
    std::vector<size_t> argIds,
    std::optional<size_t> defaultExprId,
    int arrayIndex,
    int hashtableIndex,
    EValueType resultType,
    TComparerManagerPtr comparerManager);

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
    TCodegenFragmentInfosPtr fragmentInfos,
    size_t predicateId);

size_t MakeCodegenFilterFinalizedOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t slot,
    TCodegenFragmentInfosPtr fragmentInfos,
    size_t predicateId,
    size_t keySize,
    std::vector<TCodegenAggregate> codegenAggregates,
    std::vector<EValueType> stateTypes);

size_t MakeCodegenJoinOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t slot,
    int index,
    TCodegenFragmentInfosPtr fragmentInfos,
    std::vector<std::pair<size_t, bool>> equations,
    size_t commonKeyPrefix,
    size_t foreignKeyPrefix,
    TComparerManagerPtr comparerManager);

struct TSingleJoinCGParameters
{
    std::vector<std::pair<size_t, bool>> Equations;
    size_t CommonKeyPrefix;
    size_t ForeignKeyPrefix;
    std::vector<EValueType> LookupKeyTypes;
};

size_t MakeCodegenMultiJoinOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot,
    int index,
    TCodegenFragmentInfosPtr fragmentInfos,
    std::vector<TSingleJoinCGParameters> parameters,
    std::vector<std::pair<size_t, EValueType>> primaryColumns,
    TComparerManagerPtr comparerManager);

size_t MakeCodegenGroupOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot,
    TCodegenFragmentInfosPtr fragmentInfos,
    std::vector<size_t> groupExprsIds,
    std::vector<size_t> aggregateExprIds,
    std::vector<TCodegenAggregate> codegenAggregates,
    std::vector<EValueType> keyTypes,
    std::vector<EValueType> stateTypes,
    bool isMerge,
    bool checkNulls,
    TComparerManagerPtr comparerManager);

size_t MakeCodegenFinalizeOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t slot,
    size_t keySize,
    std::vector<TCodegenAggregate> codegenAggregates,
    std::vector<EValueType> stateTypes);

size_t MakeCodegenAddStreamOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t slot,
    size_t rowSize,
    EStreamTag value);

size_t MakeCodegenOrderOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t slot,
    TCodegenFragmentInfosPtr fragmentInfos,
    std::vector<size_t> exprIds,
    std::vector<EValueType> orderColumnTypes,
    std::vector<EValueType> sourceSchema,
    const std::vector<bool>& isDesc,
    TComparerManagerPtr comparerManager);

size_t MakeCodegenProjectOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t slot,
    TCodegenFragmentInfosPtr fragmentInfos,
    std::vector<size_t> argIds);

void MakeCodegenWriteOp(
    TCodegenSource* codegenSource,
    size_t slot,
    size_t rowSize);

////////////////////////////////////////////////////////////////////////////////

TCGQueryCallback CodegenEvaluate(
    const TCodegenSource* codegenSource,
    size_t slotIndex);

TCGExpressionCallback CodegenStandaloneExpression(
    const TCodegenFragmentInfosPtr& fragmentInfos,
    size_t exprId);

TCGAggregateCallbacks CodegenAggregate(
    TCodegenAggregate codegenAggregate,
    EValueType argumentType,
    EValueType stateType);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
