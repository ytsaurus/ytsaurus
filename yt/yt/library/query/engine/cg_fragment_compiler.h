#pragma once

#include "cg_helpers.h"
#include "cg_ir_builder.h"
#include "cg_types.h"

#include <yt/yt/library/query/base/query_common.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using TCodegenSource = std::function<void(TCGOperatorContext& builder)>;
using TCodegenValue = std::function<TCGValue(TCGBaseContext& builder)>;
using TCodegenExpression = std::function<TCGValue(TCGExprContext& builder)>;

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
    : public TRefCounted
{
    std::vector<TCodegenFragmentInfo> Items;
    std::vector<Function*> Functions;
    TString NamePrefix;
};

DEFINE_REFCOUNTED_TYPE(TCodegenFragmentInfos)

using TCodegenAggregateInit = std::function<TCGValue(TCGBaseContext& builder, Value* buffer)>;
using TCodegenAggregateUpdate = std::function<TCGValue(TCGBaseContext& builder, Value* buffer, TCGValue aggState, std::vector<TCGValue> newValues)>;
using TCodegenAggregateMerge = std::function<TCGValue(TCGBaseContext& builder, Value* buffer, TCGValue dstAggState, TCGValue aggState)>;
using TCodegenAggregateFinalize = std::function<TCGValue(TCGBaseContext& builder, Value* buffer, TCGValue aggState)>;

struct TCodegenAggregate
{
    TCodegenAggregateInit Initialize;
    TCodegenAggregateUpdate Update;
    TCodegenAggregateMerge Merge;
    TCodegenAggregateFinalize Finalize;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TComparerManager)

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
    TString name,
    bool useCanonicalNullRelations);

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

TCodegenExpression MakeCodegenCaseExpr(
    std::optional<size_t> optionalOperandId,
    std::optional<EValueType> optionalOperandType,
    std::vector<std::pair<size_t, size_t>> whenThenExpressionIds,
    std::optional<size_t> defaultId,
    EValueType resultType,
    TComparerManagerPtr comparerManager);

TCodegenExpression MakeCodegenLikeExpr(
    size_t textId,
    EStringMatchOp opcode,
    size_t patternId,
    std::optional<size_t> escapeCharacterId,
    int contextIndex);

TCodegenExpression MakeCodegenCompositeMemberAccessorExpr(
    size_t compositeId,
    int nestedStructOrTupleItemAccessorOpaqueIndex,
    std::optional<size_t> dictOrListItemAccessorId,
    EValueType resultType);

////////////////////////////////////////////////////////////////////////////////

void CodegenEmptyOp(TCGOperatorContext& builder);

std::tuple<size_t, size_t> MakeCodegenDuplicateOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t slot);

size_t MakeCodegenMergeOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t firstSlot,
    size_t secondSlot);

size_t MakeCodegenOnceOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t sourceSlot);

size_t MakeCodegenScanOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    const std::vector<int>& stringLikeColumnIndices,
    int rowSchemaInformationIndex,
    NCodegen::EExecutionBackend executionBackend);

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

struct TSingleJoinCGParameters
{
    std::vector<std::pair<size_t, bool>> Equations;
    size_t CommonKeyPrefix;
    size_t ForeignKeyPrefix;
    std::vector<EValueType> LookupKeyTypes;
};

size_t MakeCodegenArrayJoinOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot,
    TCodegenFragmentInfosPtr fragmentInfos,
    std::vector<size_t> arrayIds,
    int parametersIndex,
    size_t predicateId);

size_t MakeCodegenMultiJoinOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot,
    int index,
    TCodegenFragmentInfosPtr fragmentInfos,
    std::vector<TSingleJoinCGParameters> parameters,
    std::vector<std::pair<size_t, EValueType>> primaryColumns,
    TComparerManagerPtr comparerManager);

struct TGroupOpSlots
{
    size_t Intermediate;
    size_t Final;
    size_t DeltaFinal;
    size_t Totals;
};

TGroupOpSlots MakeCodegenGroupOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot,
    TCodegenFragmentInfosPtr fragmentInfos,
    std::vector<size_t> groupExprsIds,
    std::vector<std::vector<size_t>> aggregateExprIds,
    std::vector<TCodegenAggregate> codegenAggregates,
    std::vector<EValueType> keyTypes,
    std::vector<EValueType> stateTypes,
    bool allAggregatesFirst,
    bool isMerge,
    bool checkForNullGroupKey,
    size_t commonPrefixWithPrimaryKey,
    TComparerManagerPtr comparerManager);

size_t MakeCodegenGroupTotalsOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot,
    std::vector<TCodegenAggregate> codegenAggregates,
    std::vector<EValueType> keyTypes,
    std::vector<EValueType> stateTypes);

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
    EStreamTag value,
    const std::vector<EValueType>& stateTypes);

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

size_t MakeCodegenOffsetLimiterOp(
    TCodegenSource* codegenSource,
    size_t* slotCount,
    size_t producerSlot,
    size_t offsetId,
    size_t limitId);

void MakeCodegenWriteOp(
    TCodegenSource* codegenSource,
    size_t slot,
    size_t rowSize);

////////////////////////////////////////////////////////////////////////////////

TCGQueryImage CodegenQuery(
    const TCodegenSource* codegenSource,
    size_t slotIndex,
    NCodegen::EExecutionBackend executionBackend);

TCGExpressionImage CodegenStandaloneExpression(
    const TCodegenFragmentInfosPtr& fragmentInfos,
    size_t exprId,
    NCodegen::EExecutionBackend executionBackend);

TCGAggregateImage CodegenAggregate(
    TCodegenAggregate codegenAggregate,
    std::vector<EValueType> argumentTypes,
    EValueType stateType,
    NCodegen::EExecutionBackend executionBackend);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
