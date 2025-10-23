#pragma once

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/tablet_client/public.h>

namespace NYT::NRowMerger {

////////////////////////////////////////////////////////////////////////////////

using TAggregateFunction = void(NTableClient::TUnversionedValue* state, const NTableClient::TUnversionedValue& value);

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

TNestedColumnsSchema GetNestedColumnsSchema(NTableClient::TTableSchemaPtr tableSchema);

TNestedColumnsSchema FilterNestedColumnsSchema(const TNestedColumnsSchema& nestedSchema, TRange<int> columnIds);

const TNestedKeyColumn* GetNestedColumnById(TRange<TNestedKeyColumn> keyColumns, ui16 columnId);
const TNestedValueColumn* GetNestedColumnById(TRange<TNestedValueColumn> keyColumns, ui16 columnId);

int UnpackNestedValuesList(std::vector<NTableClient::TUnversionedValue>* parsedValues, TStringBuf data, NTableClient::EValueType listItemType);

class TNestedTableMerger
{
public:
    void UnpackKeyColumns(TRange<TMutableRange<NTableClient::TVersionedValue>> keyColumns, TRange<TNestedKeyColumn> keyColumnsSchema);
    void UnpackKeyColumns(TRange<std::vector<NTableClient::TVersionedValue>> keyColumns, TRange<TNestedKeyColumn> keyColumnsSchema);

    NTableClient::TVersionedValue BuildMergedKeyColumns(int index, NTableClient::TRowBuffer* rowBuffer);
    NTableClient::TVersionedValue BuildMergedValueColumn(
        TRange<NTableClient::TVersionedValue> values,
        NTableClient::EValueType elementType,
        TAggregateFunction* aggregateFunction,
        NTableClient::TRowBuffer* rowBuffer);

private:
    std::vector<int> NestedRowCounts_;
    std::vector<int> RowIds_;

    std::vector<int> Offsets_;
    std::vector<NTableClient::TTimestamp> Timestamps_;

    std::vector<std::vector<NTableClient::TUnversionedValue>> UnpackedKeys_;

    std::vector<int> RowIdHeap_;
    std::vector<int> CurrentOffsets_;

    std::vector<NTableClient::TUnversionedValue> UnpackedValues_;
    std::vector<NTableClient::TUnversionedValue> ResultValues_;

    std::vector<int> OrderingTranslationLayer_;


    void UnpackKeyColumn(
        ui16 keyColumnId,
        int mergeStreamCount,
        TRange<NTableClient::TVersionedValue> keyColumn,
        TNestedKeyColumn keyColumnSchema);

    void Reset(
        int keyWidth,
        int mergeStreamCount);

    void BuildMergeScript();

    NTableClient::TUnversionedValue BuildMergedKeyColumns(
        TRange<int> counts,
        TRange<int> ids,
        TRange<NTableClient::TUnversionedValue> unpackedKeys,
        NTableClient::TRowBuffer* rowBuffer);

    NTableClient::TVersionedValue BuildMergedValueColumn(
        TRange<int> counts,
        TRange<int> ids,
        TRange<NTableClient::TTimestamp> timestamps,
        TRange<NTableClient::TVersionedValue> values,
        NTableClient::EValueType elementType,
        TAggregateFunction* aggregateFunction,
        NTableClient::TRowBuffer* rowBuffer);
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
