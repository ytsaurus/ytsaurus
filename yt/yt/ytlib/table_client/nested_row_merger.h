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

const TNestedKeyColumn* GetNestedColumnById(TRange<TNestedKeyColumn> keyColumns, ui16 columnId);
const TNestedValueColumn* GetNestedColumnById(TRange<TNestedValueColumn> keyColumns, ui16 columnId);

int UnpackNestedValuesList(std::vector<TUnversionedValue>* parsedValues, TStringBuf data, EValueType listItemType);

class TNestedTableMerger
{
public:
    void UnpackKeyColumns(TRange<TMutableRange<TVersionedValue>> keyColumns, TRange<TNestedKeyColumn> keyColumnsSchema);

    // Can be merged with UnpackKeyColumns.
    void BuildMergeScript();

    TVersionedValue BuildMergedKeyColumns(int index, TRowBuffer* rowBuffer);
    TVersionedValue ApplyMergeScript(
        TRange<TVersionedValue> values,
        EValueType elementType,
        TAggregateFunction* aggregateFunction,
        TRowBuffer* rowBuffer);

private:
    std::vector<int> Counts_;
    std::vector<int> Ids_;

    std::vector<int> Offsets_;
    std::vector<TTimestamp> Timestamps_;

    std::vector<std::vector<TUnversionedValue>> UnpackedKeys_;

    std::vector<int> Heap_;
    std::vector<int> CurrentOffsets_;
    // TODO(lukyan): Keep end offsets instead of offsets and evaluate start offsets each time.
    std::vector<int> EndOffsets_;

    std::vector<TUnversionedValue> UnpackedValues_;
    std::vector<TUnversionedValue> ResultValues_;

    TUnversionedValue BuildMergedKeyColumns(
        TRange<int> counts,
        TRange<int> ids,
        TRange<TUnversionedValue> unpackedKeys,
        TRowBuffer* rowBuffer);

    TVersionedValue ApplyMergeScript(
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
