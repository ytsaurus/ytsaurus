#include "stdafx.h"
#include "name_table.h"

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

int TNameTable::GetNameCount() const
{
    TGuard<TSpinLock> guard(SpinLock);
    return NameByIndex.size();
}

TNullable<int> TNameTable::FindIndex(const Stroka& name) const
{
    TGuard<TSpinLock> guard(SpinLock);
    auto it = IndexByName.find(name);
    if (it == IndexByName.end()) {
        return Null;
    } else {
        return MakeNullable(it->second);
    }
}

int TNameTable::GetIndex(const Stroka& name) const
{
    auto index = FindIndex(name);
    YCHECK(index);
    return *index;
}

const Stroka& TNameTable::GetName(int index) const
{
    TGuard<TSpinLock> guard(SpinLock);
    YCHECK(index >= 0 && index < NameByIndex.size());
    return NameByIndex[index];
}

int TNameTable::RegisterName(const Stroka& name)
{
    TGuard<TSpinLock> guard(SpinLock);
    return DoRegisterName(name);
}

int TNameTable::DoRegisterName(const Stroka& name)
{
    int index = NameByIndex.size();
    NameByIndex.push_back(name);
    YCHECK(IndexByName.insert(std::make_pair(name, index)).second);
    return index;
}

int TNameTable::GetOrRegisterName(const Stroka& name)
{
    TGuard<TSpinLock> guard(SpinLock);
    auto it = IndexByName.find(name);
    if (it == IndexByName.end()) {
        return DoRegisterName(name);
    } else {
        return it->second;
    }
}

void ToProto(NProto::TNameTableExt* protoNameTable, TNameTablePtr nameTable)
{
    protoNameTable->clear_names();
    for (int index = 0; index < nameTable->GetNameCount(); ++index) {
        protoNameTable->add_names(nameTable->GetName(index));
    }
}

void FromProto(TNameTablePtr* nameTable, const NProto::TNameTableExt& protoNameTable)
{
    *nameTable = New<TNameTable>();
    for (const auto& name: protoNameTable.names()) {
        (*nameTable)->RegisterName(name);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
