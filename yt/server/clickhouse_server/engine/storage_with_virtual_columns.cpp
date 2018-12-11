#include "storage_with_virtual_columns.h"

#include "format_helpers.h"

//#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int ILLEGAL_COLUMN;
}

}

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

namespace {

////////////////////////////////////////////////////////////////////////////////

bool FindColumnInList(
    const DB::NamesAndTypesList& columns,
    const std::string& name,
    DB::NameAndTypePair& found)
{
    for (const auto& column : columns) {
        if (name == column.name) {
            found = column;
            return true;
        }
    }
    return false;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool IStorageWithVirtualColumns::hasColumn(const std::string& name) const
{
    DB::NameAndTypePair found; // ignored
    return FindColumnImpl(name, found);
}

DB::NameAndTypePair IStorageWithVirtualColumns::getColumn(const std::string& name) const
{
    DB::NameAndTypePair found;
    if (!FindColumnImpl(name, found)) {
        throw DB::Exception(
            "Column " + Quoted(name) + " not found",
            DB::ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
    }
    return found;
}

bool IStorageWithVirtualColumns::FindColumnImpl(
    const std::string& name,
    DB::NameAndTypePair& found) const
{
    return
        FindColumnInList(ListPhysicalColumns(), name, found) ||
        FindColumnInList(ListVirtualColumns(), name, found);
}

void IStorageWithVirtualColumns::SplitColumns(
    const DB::Names& names,
    DB::Names& physical,
    DB::Names& virtual_) const
{
    const auto& allPhysicalColumns = ListPhysicalColumns();
    const auto& allVirtualColumns = ListVirtualColumns();

    physical = allPhysicalColumns.filter(names).getNames();
    virtual_ = allVirtualColumns.filter(names).getNames();

    if (names.size() != physical.size() + virtual_.size()) {
        throw DB::Exception("Illegal set of columns", DB::ErrorCodes::ILLEGAL_COLUMN);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
