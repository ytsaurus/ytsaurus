#include "table.h"

#include "types_translation.h"

#include <yt/client/table_client/schema.h>

#include <yt/core/yson/string.h>

namespace NYT::NClickHouseServer {

using namespace NTableClient;
using namespace NYson;

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

class TClickHouseTableSchemaBuilder
{
private:
    std::vector<TClickHouseColumn> Columns;

public:
    TClickHouseTableSchemaBuilder()
    {}

    // return false if column skipped
    bool AddColumn(const TColumnSchema& columnSchema);

    std::vector<TClickHouseColumn> GetColumns()
    {
        return std::move(Columns);
    }
};

////////////////////////////////////////////////////////////////////////////////

bool TClickHouseTableSchemaBuilder::AddColumn(const TColumnSchema& ytColumn)
{
    auto ytPhysicalType = ytColumn.GetPhysicalType();

    if (!IsYtTypeSupported(ytPhysicalType)) {
        // skip unsupported type
        return false;
    }

    TClickHouseColumn column;
    column.Name = ytColumn.Name();
    column.Type = RepresentYtType(ytPhysicalType);
    if (ytColumn.SortOrder()) {
        column.SetSorted();
    }
    Columns.push_back(std::move(column));
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TClickHouseTablePtr CreateClickHouseTable(
    const TString& name,
    const TTableSchema& schema)
{
    TClickHouseTableSchemaBuilder schemaBuilder;

    for (const auto& columnSchema: schema.Columns()) {
        schemaBuilder.AddColumn(columnSchema);
    }

    return std::make_shared<TClickHouseTable>(name, schemaBuilder.GetColumns());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
