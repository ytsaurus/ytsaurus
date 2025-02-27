#include "column_reader.h"

#include "boolean_column_reader.h"
#include "floating_point_column_reader.h"
#include "integer_column_reader.h"
#include "null_column_reader.h"
#include "string_column_reader.h"

#include <yt/yt/client/table_client/schema.h>

namespace NYT::NTableChunkFormat {

using namespace NTableClient;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IUnversionedColumnReader> CreateUnversionedColumnReader(
    const TColumnSchema& schema,
    const TColumnMeta& meta,
    int columnIndex,
    int columnId,
    std::optional<ESortOrder> sortOrder,
    bool serializeFloatsAsDoubles)
{
    auto doCreate = [&] (auto factory) {
        return factory(meta, columnIndex, columnId, sortOrder, schema);
    };

    switch (schema.GetWireType()) {
        case EValueType::Int64:
            return doCreate(CreateUnversionedInt64ColumnReader);

        case EValueType::Uint64:
            return doCreate(CreateUnversionedUint64ColumnReader);

        case EValueType::Double:
            switch (schema.CastToV1Type()) {
                case ESimpleLogicalValueType::Float:
                    if (!serializeFloatsAsDoubles) {
                        return doCreate(CreateUnversionedFloatingPointColumnReader<float>);
                    }
                    [[fallthrough]];
                default:
                    return doCreate(CreateUnversionedFloatingPointColumnReader<double>);
            }
        case EValueType::String:
            return doCreate(CreateUnversionedStringColumnReader);

        case EValueType::Boolean:
            return doCreate(CreateUnversionedBooleanColumnReader);

        case EValueType::Any:
            return doCreate(CreateUnversionedAnyColumnReader);

        case EValueType::Composite:
            return doCreate(CreateUnversionedCompositeColumnReader);

        case EValueType::Null:
            return doCreate(CreateUnversionedNullColumnReader);

        default:
            ThrowUnexpectedValueType(schema.GetWireType());
    }
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IVersionedColumnReader> CreateVersionedColumnReader(
    const TColumnSchema& schema,
    const TColumnMeta& meta,
    int columnId)
{
    auto doCreate = [&] (auto factory) {
        return factory(meta, columnId, schema);
    };

    switch (schema.GetWireType()) {
        case EValueType::Int64:
            return doCreate(CreateVersionedInt64ColumnReader);

        case EValueType::Uint64:
            return doCreate(CreateVersionedUint64ColumnReader);

        case EValueType::Double:
            return doCreate(CreateVersionedDoubleColumnReader);

        case EValueType::Boolean:
            return doCreate(CreateVersionedBooleanColumnReader);

        case EValueType::String:
            return doCreate(CreateVersionedStringColumnReader);

        case EValueType::Any:
            return doCreate(CreateVersionedAnyColumnReader);

        case EValueType::Composite:
            return doCreate(CreateVersionedCompositeColumnReader);

        default:
            ThrowUnexpectedValueType(schema.GetWireType());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
