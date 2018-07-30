#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/IStorage.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

// Friendly adaptation of ITableDeclaration
// Just override ListPhysicalColumns and ListVirtualColumns

class IStorageWithVirtualColumns
    : public DB::IStorage
{
public:
    bool hasColumn(const std::string& name) const override;

    DB::NameAndTypePair getColumn(const std::string& name) const override;

protected:
    virtual const DB::NamesAndTypesList& ListPhysicalColumns() const = 0;

    virtual const DB::NamesAndTypesList& ListVirtualColumns() const
    {
        static DB::NamesAndTypesList empty;
        return empty;
    }

    void SplitColumns(const DB::Names& names, DB::Names& physical, DB::Names& virtual_) const;

private:
/* TODO still need?
    const DB::NamesAndTypesList& getColumnsListImpl() const override
    {
        return ListPhysicalColumns();
    }
*/

    bool FindColumnImpl(const std::string& name, DB::NameAndTypePair& found) const;
};

} // namespace NClickHouse
} // namespace NYT

