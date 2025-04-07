#pragma once

#include "ast.h"
#include "query.h"
#include "public.h"

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct TQueryPreparerBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

EValueType GetType(const NAst::TLiteralValue& literalValue);

TValue GetValue(const NAst::TLiteralValue& literalValue);

void BuildRow(
    TUnversionedRowBuilder* rowBuilder,
    const NAst::TLiteralValueTuple& tuple,
    const std::vector<EValueType>& argTypes,
    TStringBuf source);

TSharedRange<TRow> LiteralTupleListToRows(
    const NAst::TLiteralValueTupleList& literalTuples,
    const std::vector<EValueType>& argTypes,
    TStringBuf source);

TSharedRange<TRowRange> LiteralRangesListToRows(
    const NAst::TLiteralValueRangeList& literalRanges,
    const std::vector<EValueType>& argTypes,
    TStringBuf source);

////////////////////////////////////////////////////////////////////////////////

TConstExpressionPtr ApplyRewriters(TConstExpressionPtr expr);

////////////////////////////////////////////////////////////////////////////////

std::optional<TValue> FoldConstants(
    EUnaryOp opcode,
    const TConstExpressionPtr& operand);

std::optional<TValue> FoldConstants(
    EBinaryOp opcode,
    const TConstExpressionPtr& lhsExpr,
    const TConstExpressionPtr& rhsExpr);

////////////////////////////////////////////////////////////////////////////////

TString InferReferenceName(const NAst::TColumnReference& ref);

struct TTable
{
    const TTableSchema& Schema;
    std::optional<TString> Alias;
    std::vector<TColumnDescriptor>* Mapping = nullptr;
};

struct TColumnEntry
{
    TLogicalTypePtr LogicalType;

    size_t LastTableIndex;
    size_t OriginTableIndex;
};

struct IReferenceResolver
{
    virtual TLogicalTypePtr Resolve(const NAst::TColumnReference& reference) = 0;
    virtual void PopulateAllColumns(IReferenceResolver* targetResolver) = 0;

    virtual ~IReferenceResolver() = default;
};

std::unique_ptr<IReferenceResolver> CreateColumnResolver(
    const TTableSchema* schema,
    std::optional<TString> alias,
    std::vector<TColumnDescriptor>* mapping);

std::unique_ptr<IReferenceResolver> CreateJoinColumnResolver(
    std::unique_ptr<IReferenceResolver> parentProvider,
    const TTableSchema* schema,
    std::optional<TString> alias,
    std::vector<TColumnDescriptor>* mapping,
    THashSet<std::string>* selfJoinedColumns,
    THashSet<std::string>* foreignJoinedColumns,
    THashSet<std::string> commonColumnNames);

class TExprBuilder
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TStringBuf, Source);
    DEFINE_BYREF_RO_PROPERTY(TConstTypeInferrerMapPtr, Functions);

    std::unique_ptr<IReferenceResolver> ColumnResolver;

    TExprBuilder(
        TStringBuf source,
        const TConstTypeInferrerMapPtr& functions);

    void SetGroupData(const TNamedItemList* groupItems, TAggregateItemList* aggregateItems);

    TLogicalTypePtr GetColumnType(const NAst::TColumnReference& reference);

    virtual TConstExpressionPtr DoBuildTypedExpression(
        const NAst::TExpression* expr, TRange<EValueType> resultTypes) = 0;

    virtual TConstExpressionPtr BuildTypedExpression(
        const NAst::TExpression* expr,
        TRange<EValueType> resultTypes = {
            EValueType::Null,
            EValueType::Int64,
            EValueType::Uint64,
            EValueType::Double,
            EValueType::Boolean,
            EValueType::String,
            EValueType::Any,
            EValueType::Composite
        });

    virtual ~TExprBuilder() = default;

protected:
    // TODO: Combine in Structure? Move out?
    const TNamedItemList* GroupItems_ = nullptr;
    // TODO: Enrich TMappedSchema with alias and keep here pointers to TMappedSchema.

    TAggregateItemList* AggregateItems_ = nullptr;

    bool AfterGroupBy_ = false;
};

std::unique_ptr<TExprBuilder> CreateExpressionBuilder(
    TStringBuf source,
    const TConstTypeInferrerMapPtr& functions,
    const NAst::TAliasMap& aliasMap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
