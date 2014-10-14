#include "stdafx.h"
#include "row_base.h"

#include <core/misc/error.h>
#include <core/misc/small_vector.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

void ValidateDataValueType(EValueType type)
{
    // TODO(babenko): handle any
    if (type != EValueType::Int64 &&
        type != EValueType::Uint64 &&
        type != EValueType::Double &&
        type != EValueType::Boolean &&
        type != EValueType::String &&
        type != EValueType::Null)
    {
        THROW_ERROR_EXCEPTION("Invalid date value type %Qlv", type);
    }       
}

void ValidateKeyValueType(EValueType type)
{
    // TODO(babenko): handle any
    if (type != EValueType::Int64 &&
        type != EValueType::Uint64 &&
        type != EValueType::Double &&
        type != EValueType::Boolean &&
        type != EValueType::String &&
        type != EValueType::Null &&
        type != EValueType::Min &&
        type != EValueType::Max)
    {
        THROW_ERROR_EXCEPTION("Invalid key value type %Qlv", type);
    }       
}

void ValidateSchemaValueType(EValueType type)
{
    // TODO(babenko): handle any
    if (type != EValueType::Int64 &&
        type != EValueType::Uint64 &&
        type != EValueType::Double &&
        type != EValueType::Boolean &&
        type != EValueType::String)
    {
        THROW_ERROR_EXCEPTION("Invalid schema value type %Qlv", type);
    }       
}

void ValidateColumnFilter(const TColumnFilter& columnFilter, int schemaColumnCount)
{
    if (columnFilter.All)
        return;

    SmallVector<bool, TypicalColumnCount> flags;
    flags.resize(schemaColumnCount);

    for (int index : columnFilter.Indexes) {
        if (index < 0 || index >= schemaColumnCount) {
            THROW_ERROR_EXCEPTION("Column filter contains invalid index: actual %v, expected in range [0, %v]",
                index,
                schemaColumnCount - 1);
        }
        if (flags[index]) {
            THROW_ERROR_EXCEPTION("Column filter contains duplicate index %v",
                index);
        }
        flags[index] = true;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
