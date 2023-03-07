#include "unversioned_value_yson_writer.h"

#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/unversioned_value.h>

#include <yt/core/yson/pull_parser.h>

namespace NYT::NFormats {

using namespace NYson;
using namespace NTableClient;
using namespace NComplexTypes;

////////////////////////////////////////////////////////////////////////////////

TUnversionedValueYsonWriter::TUnversionedValueYsonWriter(
    const TNameTablePtr& nameTable,
    const TTableSchema& tableSchema,
    EComplexTypeMode complexTypeMode,
    bool skipNullValues)
{
    if (complexTypeMode == EComplexTypeMode::Positional) {
        return;
    }

    TPositionalToNamedConfig config;
    config.SkipNullValues = skipNullValues;

    const auto& columns = tableSchema.Columns();
    for (size_t i = 0; i != columns.size(); ++i) {
        const auto& column = columns[i];
        if (column.SimplifiedLogicalType()) {
            continue;
        }
        auto id = nameTable->GetIdOrRegisterName(column.Name());
        TComplexTypeFieldDescriptor descriptor(column.Name(), column.LogicalType());
        ColumnConverters_[id] = CreatePositionalToNamedYsonConverter(descriptor, config);
    }
}

void TUnversionedValueYsonWriter::WriteValue(const TUnversionedValue& value, IYsonConsumer* consumer)
{
    switch (value.Type) {
        case EValueType::Int64:
            consumer->OnInt64Scalar(value.Data.Int64);
            return;
        case EValueType::Uint64:
            consumer->OnUint64Scalar(value.Data.Uint64);
            return;
        case EValueType::Double:
            consumer->OnDoubleScalar(value.Data.Double);
            return;
        case EValueType::Boolean:
            consumer->OnBooleanScalar(value.Data.Boolean);
            return;
        case EValueType::String:
            consumer->OnStringScalar(TStringBuf(value.Data.String, value.Length));
            return;
        case EValueType::Null:
            consumer->OnEntity();
            return;
        case EValueType::Any:
            consumer->OnRaw(TStringBuf(value.Data.String, value.Length), EYsonType::Node);
            return;
        case EValueType::Composite: {
            auto data = TStringBuf(value.Data.String, value.Length);
            auto it = ColumnConverters_.find(value.Id);
            if (it != ColumnConverters_.end()) {
                const auto& converter = ColumnConverters_[value.Id];
                if (converter) {
                    ApplyYsonConverter(converter, data, consumer);
                    return;
                }
            }
            consumer->OnRaw(data, EYsonType::Node);
            return;
        }
        case EValueType::Min:
        case EValueType::TheBottom:
        case EValueType::Max:
            break;
    }
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
