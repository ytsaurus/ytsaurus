#pragma once

#include "public.h"

#include <yt/yt/client/tablet_client/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

using TAggregateFunction = void(TUnversionedValue* state, const TUnversionedValue& value);

struct TNestedKeyColumn
{
    ui16 Id;
    EValueType Type;
};

struct TNestedValueColumn
{
    ui16 Id;
    EValueType Type;
    TAggregateFunction* AggregateFunction;
};

struct TNestedColumnsSchema
{
    std::vector<TNestedKeyColumn> KeyColumns;
    std::vector<TNestedValueColumn> ValueColumns;
};

TNestedColumnsSchema GetNestedColumnsSchema(TTableSchemaPtr tableSchema);

TNestedColumnsSchema FilterNestedColumnsSchema(const TNestedColumnsSchema& nestedSchema, TRange<int> columnIds);

const TNestedKeyColumn* GetNestedColumnById(TRange<TNestedKeyColumn> keyColumns, ui16 columnId);
const TNestedValueColumn* GetNestedColumnById(TRange<TNestedValueColumn> keyColumns, ui16 columnId);

int UnpackNestedValuesList(std::vector<TUnversionedValue>* parsedValues, TStringBuf data, EValueType listItemType);

class TNestedTableMerger
{
public:
    void UnpackKeyColumns(TRange<TMutableRange<TVersionedValue>> keyColumns, TRange<TNestedKeyColumn> keyColumnsSchema);
    void UnpackKeyColumns(TRange<std::vector<TVersionedValue>> keyColumns, TRange<TNestedKeyColumn> keyColumnsSchema);

    TVersionedValue BuildMergedKeyColumns(int index, TRowBuffer* rowBuffer);
    TVersionedValue BuildMergedValueColumn(
        TRange<TVersionedValue> values,
        EValueType elementType,
        TAggregateFunction* aggregateFunction,
        TRowBuffer* rowBuffer);

private:
    std::vector<int> NestedRowCounts_;
    std::vector<int> RowIds_;

    std::vector<int> Offsets_;
    std::vector<TTimestamp> Timestamps_;

    std::vector<std::vector<TUnversionedValue>> UnpackedKeys_;

    std::vector<int> RowIdHeap_;
    std::vector<int> CurrentOffsets_;

    std::vector<TUnversionedValue> UnpackedValues_;
    std::vector<TUnversionedValue> ResultValues_;

    std::vector<int> OrderingTranslationLayer_;


    void UnpackKeyColumn(
        ui16 keyColumnId,
        int mergeStreamCount,
        TRange<TVersionedValue> keyColumn,
        TNestedKeyColumn keyColumnSchema);

    void Reset(
        int keyWidth,
        int mergeStreamCount);

    void BuildMergeScript();

    TUnversionedValue BuildMergedKeyColumns(
        TRange<int> counts,
        TRange<int> ids,
        TRange<TUnversionedValue> unpackedKeys,
        TRowBuffer* rowBuffer);

    TVersionedValue BuildMergedValueColumn(
        TRange<int> counts,
        TRange<int> ids,
        TRange<TTimestamp> timestamps,
        TRange<TVersionedValue> values,
        EValueType elementType,
        TAggregateFunction* aggregateFunction,
        TRowBuffer* rowBuffer);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
