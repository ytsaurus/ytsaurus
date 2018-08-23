#include "system_columns.h"

namespace NInterop {

////////////////////////////////////////////////////////////////////////////////

TColumnList TSystemColumns::ToColumnList() const
{
    TColumnList columns;
    columns.reserve(GetCount());

    if (TableName.Defined()) {
        columns.emplace_back(*TableName, EColumnType::String);
    }

    return columns;
}

} // namespace NInterop
