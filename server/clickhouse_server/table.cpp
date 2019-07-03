#include "table.h"

#include "type_translation.h"

#include <yt/client/table_client/schema.h>

#include <yt/core/yson/string.h>

namespace NYT::NClickHouseServer {

using namespace NTableClient;
using namespace NYson;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

bool TClickHouseColumn::IsSorted() const
{
    return (Flags & static_cast<int>(EColumnFlags::Sorted)) != 0;
}

bool TClickHouseColumn::IsNullable() const
{
    return (Flags & static_cast<int>(EColumnFlags::Nullable)) != 0;
}

void TClickHouseColumn::SetSorted()
{
    Flags |= static_cast<int>(EColumnFlags::Sorted);
}

void TClickHouseColumn::DropSorted()
{
    Flags &= 0x03 ^ static_cast<int>(EColumnFlags::Sorted);
}

void TClickHouseColumn::SetNullable()
{
    Flags |= static_cast<int>(EColumnFlags::Nullable);
}

std::optional<TClickHouseColumn> TClickHouseColumn::FromColumnSchema(const NTableClient::TColumnSchema& columnSchema)
{
    auto type = columnSchema.GetPhysicalType();

    if (!IsYtTypeSupported(type)) {
        return std::nullopt;
    }

    TClickHouseColumn column;
    column.Name = columnSchema.Name();
    column.Type = RepresentYtType(type);
    if (columnSchema.SortOrder()) {
        column.SetSorted();
    }
    if (!columnSchema.Required()) {
        column.SetNullable();
    }

    return column;
}

////////////////////////////////////////////////////////////////////////////////

bool operator == (const TClickHouseColumn& lhs, const TClickHouseColumn& rhs)
{
    return lhs.Name == rhs.Name &&
        lhs.Type == rhs.Type &&
        lhs.Flags == rhs.Flags;
}

bool operator != (const TClickHouseColumn& lhs, const TClickHouseColumn& rhs)
{
    return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////////////

TClickHouseTable::TClickHouseTable(const TRichYPath& path, const TTableSchema& schema)
    : Path(path)
    , TableSchema(schema)
{
    for (const auto& columnSchema : schema.Columns()) {
        if (auto clickHouseColumn = TClickHouseColumn::FromColumnSchema(columnSchema)) {
            Columns.emplace_back(*clickHouseColumn);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
