#include "table.h"

#include "types_translation.h"

#include <yt/client/table_client/schema.h>

#include <yt/core/yson/string.h>

namespace NYT {
namespace NClickHouseServer {

using namespace NTableClient;
using namespace NYson;

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

////////////////////////////////////////////////////////////////////////////////

class TClickHouseTableSchemaBuilder
{
private:
    std::vector<TColumn> Columns;

public:
    TClickHouseTableSchemaBuilder()
    {}

    // return false if column skipped
    bool AddColumn(const TColumnSchema& columnSchema);

    std::vector<TColumn> GetColumns()
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

    TColumn column;
    column.Name = ytColumn.Name();
    column.Type = RepresentYtType(ytPhysicalType);
    if (ytColumn.SortOrder()) {
        column.SetSorted();
    }
    Columns.push_back(std::move(column));
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TTablePtr CreateTable(
    const TString& name,
    const TTableSchema& schema)
{
    TClickHouseTableSchemaBuilder schemaBuilder;

    for (const auto& columnSchema: schema.Columns()) {
        schemaBuilder.AddColumn(columnSchema);
    }

    return std::make_shared<TTable>(name, schemaBuilder.GetColumns());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NClickHouseServer
} // namespace NYT
