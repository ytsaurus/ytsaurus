#include "stdafx.h"
#include "name_table.h"
#include "schema.h"

#include <ytlib/new_table_client/chunk_meta.pb.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

TNameTablePtr TNameTable::FromSchema(const TTableSchema& schema, bool excludeComputedColumns)
{
    auto nameTable = New<TNameTable>();
    for (const auto& column : schema.Columns()) {
        if (!excludeComputedColumns || !column.Expression) {
            nameTable->RegisterName(column.Name);
        }
    }
    return nameTable;
}

TNameTablePtr TNameTable::FromKeyColumns(const TKeyColumns& keyColumns)
{
    auto nameTable = New<TNameTable>();
    for (const auto& name : keyColumns) {
        nameTable->RegisterName(name);
    }
    return nameTable;
}

int TNameTable::GetSize() const
{
    TGuard<TSpinLock> guard(SpinLock_);
    return IdToName_.size();
}

TNullable<int> TNameTable::FindId(const TStringBuf& name) const
{
    TGuard<TSpinLock> guard(SpinLock_);
    auto it = NameToId_.find(name);
    if (it == NameToId_.end()) {
        return Null;
    } else {
        return MakeNullable(it->second);
    }
}

int TNameTable::GetId(const TStringBuf& name) const
{
    auto index = FindId(name);
    YCHECK(index);
    return *index;
}

const Stroka& TNameTable::GetName(int id) const
{
    TGuard<TSpinLock> guard(SpinLock_);
    YCHECK(id >= 0 && id < IdToName_.size());
    return IdToName_[id];
}

int TNameTable::RegisterName(const TStringBuf& name)
{
    TGuard<TSpinLock> guard(SpinLock_);
    return DoRegisterName(name);
}

int TNameTable::GetIdOrRegisterName(const TStringBuf& name)
{
    TGuard<TSpinLock> guard(SpinLock_);
    auto it = NameToId_.find(name);
    if (it == NameToId_.end()) {
        return DoRegisterName(name);
    } else {
        return it->second;
    }
}

int TNameTable::DoRegisterName(const TStringBuf& name)
{
    int id = IdToName_.size();
    IdToName_.emplace_back(name);
    const auto& savedName = IdToName_.back();
    YCHECK(NameToId_.insert(std::make_pair(savedName, id)).second);
    return id;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TNameTableExt* protoNameTable, const TNameTablePtr& nameTable)
{
    protoNameTable->clear_names();
    for (int id = 0; id < nameTable->GetSize(); ++id) {
        protoNameTable->add_names(nameTable->GetName(id));
    }
}

void FromProto(TNameTablePtr* nameTable, const NProto::TNameTableExt& protoNameTable)
{
    *nameTable = New<TNameTable>();
    for (const auto& name : protoNameTable.names()) {
        (*nameTable)->RegisterName(name);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
