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

struct TNameSource
{
    const TTableSchema& Schema;
    std::optional<TString> Alias;
    std::vector<TColumnDescriptor>* Mapping = nullptr;

    // Columns inherited from previous sources.
    THashSet<std::string>* SelfJoinedColumns = nullptr;
    // Columns from current source.
    THashSet<std::string>* ForeignJoinedColumns = nullptr;
    THashSet<std::string> SharedColumns = {};
};

class TReferenceResolver
{
public:
    void AddTable(TNameSource nameSource);

    TLogicalTypePtr Resolve(const NAst::TColumnReference& reference);

    void PopulateAllColumns();

    void Finish();

private:
    struct TResolvedInfo
    {
        TLogicalTypePtr Type;
        int SourceIndex;
    };

    THashMap<
        NAst::TColumnReference,
        TResolvedInfo,
        NAst::TColumnReferenceHasher,
        NAst::TColumnReferenceEqComparer> Lookup_;

    std::vector<TNameSource> NameSources_;
};

class TExprBuilder
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TStringBuf, Source);
    DEFINE_BYREF_RO_PROPERTY(TConstTypeInferrerMapPtr, Functions);

    TExprBuilder(
        TStringBuf source,
        const TConstTypeInferrerMapPtr& functions);

    virtual void AddTable(TNameSource nameSource) = 0;
    virtual TLogicalTypePtr ResolveColumn(const NAst::TColumnReference& reference) = 0;
    virtual void PopulateAllColumns() = 0;
    virtual void Finish() = 0;

    virtual void SetGroupData(const TNamedItemList* groupItems, TAggregateItemList* aggregateItems) = 0;

    virtual TString InferGroupItemName(
        const TConstExpressionPtr& typedExpression,
        const NAst::TExpression& expressionsAst) = 0;

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
};

std::unique_ptr<TExprBuilder> CreateExpressionBuilder(
    TStringBuf source,
    const TConstTypeInferrerMapPtr& functions,
    const NAst::TAliasMap& aliasMap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
