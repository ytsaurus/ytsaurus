#pragma once

#include "public.h"

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NRowMerger {

////////////////////////////////////////////////////////////////////////////////

using TAggregateFunction = void(NTableClient::TUnversionedValue* state, const NTableClient::TUnversionedValue& value);

TAggregateFunction* GetSimpleAggregateFunction(TStringBuf name, NTableClient::EValueType type);

////////////////////////////////////////////////////////////////////////////////

struct TNestedKeyColumn
{
    ui16 Id;
    NTableClient::EValueType Type;
};

struct TNestedValueColumn
{
    ui16 Id;
    NTableClient::EValueType Type;
    TAggregateFunction* AggregateFunction;
};

struct TNestedColumnsSchema
{
    std::vector<TNestedKeyColumn> KeyColumns;
    std::vector<TNestedValueColumn> ValueColumns;
};

TNestedColumnsSchema GetNestedColumnsSchema(const NTableClient::TTableSchema& tableSchema);

TNestedColumnsSchema FilterNestedColumnsSchema(const TNestedColumnsSchema& nestedSchema, TRange<int> columnIds);

const TNestedKeyColumn* FindNestedColumnById(TRange<TNestedKeyColumn> keyColumns, ui16 columnId);
const TNestedValueColumn* FindNestedColumnById(TRange<TNestedValueColumn> keyColumns, ui16 columnId);

int UnpackNestedValuesList(std::vector<NTableClient::TUnversionedValue>* parsedValues, TStringBuf data, NTableClient::EValueType listItemType);

////////////////////////////////////////////////////////////////////////////////

struct TNestedRowDiscardPolicy
    : public NYTree::TYsonStruct
{
    bool DiscardRowsWithZeroValues;
    double FloatingPointTolerance;

    REGISTER_YSON_STRUCT(TNestedRowDiscardPolicy);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNestedRowDiscardPolicy)

////////////////////////////////////////////////////////////////////////////////

class TSimpleOutputBuffer
{
public:
    ui32 RemainingBytes() const;

    char* Current() const;

    void Advance(ui32 count);

    void PushBack(char ch);

    void Write(const void* data, ui32 length);

    void Clear();

    void Reserve(ui32 count);

    TStringBuf GetBuffer() const;

private:
    std::unique_ptr<char[]> Data_;
    char* CurrentPtr_ = nullptr;
    ui32 Capacity_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TNestedTableMerger
{
public:
    explicit TNestedTableMerger(bool orderNestedRows, bool useFastYsonRoutines = false);

    // Unpack key columns and build merge script.
    // Method for schemaful and versioned row mergers.
    void UnpackKeyColumns(
        TRange<TRange<NTableClient::TVersionedValue>> keyColumns,
        TRange<TNestedKeyColumn> keyColumnsSchema);
    // Method for unversioned row merger.
    void UnpackKeyColumns(
        TRange<std::vector<NTableClient::TVersionedValue>> keyColumns,
        TRange<TNestedKeyColumn> keyColumnsSchema);

    // Unpack and merge.
    void UnpackValueColumn(
        TRange<NTableClient::TVersionedValue> values,
        NTableClient::EValueType elementType,
        TAggregateFunction* aggregateFunction);

    void DiscardZeroes(const TNestedRowDiscardPolicyPtr& nestedRowDiscardPolicy);

    NTableClient::TVersionedValue GetPackedKeyColumn(
        int index,
        NTableClient::EValueType type,
        TChunkedMemoryPool* memoryPool);
    NTableClient::TVersionedValue GetPackedValueColumn(
        int index,
        NTableClient::EValueType type,
        TChunkedMemoryPool* memoryPool);

private:
    std::vector<int> NestedRowCounts_;
    std::vector<int> RowIds_;

    std::vector<int> EndOffsets_;
    std::vector<NTableClient::TTimestamp> Timestamps_;
    std::vector<std::vector<NTableClient::TUnversionedValue>> UnpackedKeys_;

    std::vector<int> RowIdHeap_;

    // Used temporarily, keep here to reuse.
    std::vector<int> CurrentOffsets_;

    // Temp buffer.
    std::vector<NTableClient::TUnversionedValue> ValueBuffer_;

    std::vector<std::vector<NTableClient::TUnversionedValue>> ResultKeys_;
    std::vector<std::vector<NTableClient::TUnversionedValue>> ResultValues_;
    std::vector<char> Discarded_;

    TSimpleOutputBuffer PackBuffer_;

    const bool OrderNestedRows_;
    const bool UseFastYsonRoutines_ = false;

    NTableClient::TUnversionedValue PackValuesFast(
        TRange<NTableClient::TUnversionedValue> values,
        TRange<char> discard,
        NTableClient::EValueType type,
        TChunkedMemoryPool* memoryPool);

    void UnpackKeyColumn(
        ui16 keyColumnId,
        int mergeStreamCount,
        TRange<NTableClient::TVersionedValue> keyColumn,
        TNestedKeyColumn keyColumnSchema);

    void Reset(
        int keyWidth,
        int mergeStreamCount);

    void BuildMergeScript();

    void ApplyMergeScriptToKeys(int keyWidth);
};

////////////////////////////////////////////////////////////////////////////////

std::vector<int> GetMissingNestedKeyColumnsIfNeeded(
    const NTableClient::TColumnFilter& columnFilter,
    const TNestedColumnsSchema& nestedSchema);

NTableClient::TColumnFilter EnrichColumnFilter(
    const NTableClient::TColumnFilter& columnFilter,
    const TNestedColumnsSchema& nestedSchema,
    int requiredKeyColumnCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRowMerger
