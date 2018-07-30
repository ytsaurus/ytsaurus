#include "table_schema.h"

namespace NInterop {

////////////////////////////////////////////////////////////////////////////////

bool TColumn::IsSorted() const
{
    return (Flags & static_cast<int>(EColumnFlags::Sorted)) != 0;
}

bool TColumn::IsNullable() const
{
    return (Flags & static_cast<int>(EColumnFlags::Nullable)) != 0;
}


void TColumn::SetSorted()
{
    Flags |= static_cast<int>(EColumnFlags::Sorted);
}

void TColumn::SetNullable()
{
    Flags |= static_cast<int>(EColumnFlags::Nullable);
}

////////////////////////////////////////////////////////////////////////////////

bool operator == (const TColumn& lhs, const TColumn& rhs)
{
    return lhs.Name == rhs.Name &&
           lhs.Type == rhs.Type &&
           lhs.Flags == rhs.Flags;
}

bool operator != (const TColumn& lhs, const TColumn& rhs)
{
    return !(lhs == rhs);
}

} // namespace NInterop
