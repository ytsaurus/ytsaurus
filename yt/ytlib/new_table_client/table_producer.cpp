#include "stdafx.h"

#include "table_producer.h"

#include "name_table.h"

#include <core/yson/consumer.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void ProduceRow(NYson::IYsonConsumer* consumer, TUnversionedRow row, TNameTablePtr nameTable)
{
    consumer->OnListItem();
    consumer->OnBeginMap();
    for (int i = 0; i < row.GetCount(); ++i) {
        const auto& value = row[i];
        if (value.Type == EValueType::Null)
            continue;
        consumer->OnKeyedItem(nameTable->GetName(value.Id));
        switch (value.Type) {
            case EValueType::Int64:
                consumer->OnInt64Scalar(value.Data.Int64);
                break;
            case EValueType::Double:
                consumer->OnDoubleScalar(value.Data.Double);
                break;
            case EValueType::String:
                consumer->OnStringScalar(TStringBuf(value.Data.String, value.Length));
                break;
            case EValueType::Any:
                consumer->OnRaw(TStringBuf(value.Data.String, value.Length), EYsonType::Node);
                break;
            default:
                YUNREACHABLE();
        }
    }
    consumer->OnEndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
