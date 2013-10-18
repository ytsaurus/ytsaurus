#include "name_table.h"

#include <yt/ytlib/new_table_client/chunk_meta.pb.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

int TNameTable::GetNameCount() const
{
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

const Stroka& TNameTable::GetName(int index) const
{
    TGuard<TSpinLock> guard(SpinLock);

    YCHECK(index < NameByIndex.size());
    return NameByIndex[index];
}

int TNameTable::RegisterName(const Stroka& name)
{
    TGuard<TSpinLock> guard(SpinLock);

    int index = GetNameCount();
    NameByIndex.push_back(name);
    YCHECK(IndexByName.insert(std::make_pair(name, index)).second);

    return index;
}

void ToProto(NProto::TNameTable* protoNameTable, const TNameTablePtr& nameTable)
{
    protoNameTable->clear_names();
    for (int index = 0; index < nameTable->GetNameCount(); ++index) {
        protoNameTable->add_names(nameTable->GetName(index));
    }
}

TNameTablePtr FromProto(const NProto::TNameTable& protoNameTable)
{
    auto nameTable = New<TNameTable>();
    for (const auto& name: protoNameTable.names()) {
        nameTable->RegisterName(name);
    }
    return nameTable;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT

