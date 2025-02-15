#pragma once

#include "ast.h"
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

struct TBaseColumn
{
    TBaseColumn(const std::string& name, TLogicalTypePtr type)
        : Name(name)
        , LogicalType(type)
    { }

    std::string Name;
    TLogicalTypePtr LogicalType;
};

struct TTable
{
    const TTableSchema& Schema;
    std::optional<TString> Alias;
    std::vector<TColumnDescriptor>* Mapping = nullptr;
};

struct TColumnEntry
{
    TBaseColumn Column;

    size_t LastTableIndex;
    size_t OriginTableIndex;
};

class TExprBuilder
{
public:
    //! Lookup is a cache of resolved columns.
    using TLookup = THashMap<
        NAst::TReference,
        TColumnEntry,
        NAst::TCompositeAgnosticReferenceHasher,
        NAst::TCompositeAgnosticReferenceEqComparer>;

    DEFINE_BYREF_RO_PROPERTY(TLookup, Lookup);
    DEFINE_BYVAL_RO_PROPERTY(TStringBuf, Source);
    DEFINE_BYREF_RO_PROPERTY(TConstTypeInferrerMapPtr, Functions);

    TExprBuilder(
        TStringBuf source,
        const TConstTypeInferrerMapPtr& functions);

    void AddTable(
        const TTableSchema& schema,
        std::optional<TString> alias,
        std::vector<TColumnDescriptor>* mapping);

    // Columns already presented in Lookup are shared.
    // In mapping presented all columns needed for read and renamed schema.
    // SelfJoinedColumns and ForeignJoinedColumns are built from Lookup using OriginTableIndex and LastTableIndex.
    void Merge(TExprBuilder& other);

    void PopulateAllColumns();

    void SetGroupData(const TNamedItemList* groupItems, TAggregateItemList* aggregateItems);

    static const std::optional<TBaseColumn> FindColumn(const TNamedItemList& schema, const std::string& name);

    std::optional<TBaseColumn> GetColumnPtr(const NAst::TReference& reference);

    virtual TConstExpressionPtr DoBuildTypedExpression(
        const NAst::TExpression* expr, TRange<EValueType> resultTypes) = 0;

    virtual TConstExpressionPtr BuildTypedExpression(
        const NAst::TExpression* expr, TRange<EValueType> resultTypes = {
            EValueType::Null,
            EValueType::Int64,
            EValueType::Uint64,
            EValueType::Double,
            EValueType::Boolean,
            EValueType::String,
            EValueType::Any,
            EValueType::Composite});


    virtual ~TExprBuilder() = default;

protected:
    // TODO: Combine in Structure? Move out?
    const TNamedItemList* GroupItems_ = nullptr;
    // TODO: Enrich TMappedSchema with alias and keep here pointers to TMappedSchema.
    std::vector<TTable> Tables_;
    TAggregateItemList* AggregateItems_ = nullptr;

    bool AfterGroupBy_ = false;

private:
    void CheckNoOtherColumn(const NAst::TReference& reference, size_t startTableIndex) const;

    std::pair<const TTable*, TLogicalTypePtr> ResolveColumn(const NAst::TReference& reference) const;
};

std::unique_ptr<TExprBuilder> CreateExpressionBuilder(
    TStringBuf source,
    const TConstTypeInferrerMapPtr& functions,
    const NAst::TAliasMap& aliasMap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
