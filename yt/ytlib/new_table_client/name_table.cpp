#include "stdafx.h"
#include "name_table.h"

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

int TNameTable::GetNameCount() const
{
    TGuard<TSpinLock> guard(SpinLock);
    return IndexToName.size();
}

TNullable<int> TNameTable::FindIndex(const TStringBuf& name) const
{
    TGuard<TSpinLock> guard(SpinLock);
    auto it = NameToIndex.find(name);
    if (it == NameToIndex.end()) {
        return Null;
    } else {
        return MakeNullable(it->second);
    }
}

int TNameTable::GetIndex(const TStringBuf& name) const
{
    auto index = FindIndex(name);
    YCHECK(index);
    return *index;
}

const Stroka& TNameTable::GetName(int index) const
{
    TGuard<TSpinLock> guard(SpinLock);
    YCHECK(index >= 0 && index < IndexToName.size());
    return IndexToName[index];
}

int TNameTable::RegisterName(const TStringBuf& name)
{
    TGuard<TSpinLock> guard(SpinLock);
    return DoRegisterName(name);
}

int TNameTable::DoRegisterName(const TStringBuf& name)
{
    int index = IndexToName.size();
    Stroka stringName(name);
    IndexToName.push_back(stringName);
    YCHECK(NameToIndex.insert(std::make_pair(stringName, index)).second);
    return index;
}

int TNameTable::GetOrRegisterName(const TStringBuf& name)
{
    TGuard<TSpinLock> guard(SpinLock);
    auto it = NameToIndex.find(name);
    if (it == NameToIndex.end()) {
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
