#pragma once

#include "public.h"

#include <core/misc/nullable.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class TNameTable
    : public virtual TRefCounted
{
public:
    int RegisterName(const Stroka& name);
    int GetOrRegister(const Stroka& name);

    TNullable<int> FindIndex(const Stroka& name) const;
    const Stroka& GetName(int index) const;

    int GetNameCount() const;

private:
    TSpinLock SpinLock;

    yhash_map<Stroka, int> IndexByName;
    std::vector<Stroka> NameByIndex;

};

////////////////////////////////////////////////////////////////////////////////

namespace NProto { class TNameTableExt; }
void ToProto(NProto::TNameTableExt* protoNameTable, const TNameTablePtr& nameTable);
TNameTablePtr FromProto(const NProto::TNameTableExt& protoNameTable);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
