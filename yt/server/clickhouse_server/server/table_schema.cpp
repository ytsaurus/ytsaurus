#include "table_schema.h"

#include "types_translation.h"

#include <yt/client/table_client/schema.h>

#include <yt/core/yson/string.h>

namespace NYT {
namespace NClickHouse {

using namespace NYT::NTableClient;
using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

class TTableSchemaBuilder
{
private:
    NInterop::TColumnList Columns;

public:
    TTableSchemaBuilder()
    {}

    // return false if column skipped
    bool AddColumn(const TColumnSchema& columnSchema);

    NInterop::TColumnList GetColumns()
    {
        return std::move(Columns);
    }
};

////////////////////////////////////////////////////////////////////////////////

bool TTableSchemaBuilder::AddColumn(const TColumnSchema& ytColumn)
{
    auto ytPhysicalType = ytColumn.GetPhysicalType();

    if (!IsYtTypeSupported(ytPhysicalType)) {
        // skip unsupported type
        return false;
    }

    NInterop::TColumn column;
    column.Name = ytColumn.Name();
    column.Type = RepresentYtType(ytPhysicalType);
    if (ytColumn.SortOrder().HasValue()) {
        column.SetSorted();
    }
    Columns.push_back(std::move(column));
    return true;
}

////////////////////////////////////////////////////////////////////////////////

NInterop::TTablePtr CreateTableSchema(
    const TString& name,
    const TTableSchema& schema)
{
    TTableSchemaBuilder schemaBuilder;

    for (const auto& columnSchema: schema.Columns()) {
        schemaBuilder.AddColumn(columnSchema);
    }

    return std::make_shared<NInterop::TTable>(name, schemaBuilder.GetColumns());
}

}   // namespace NClickHouse
}   // namespace NYT
