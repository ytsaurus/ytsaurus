#pragma once

#include "public.h"

#include <core/misc/nullable.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class TNameTable
    : public TRefCounted
{
public:
    int RegisterName(const Stroka& name);

    TNullable<int> FindIndex(const Stroka& name) const;
    const Stroka& GetName(int index) const;

    int GetNameCount() const;

private:
    TSpinLock SpinLock;

    yhash_map<Stroka, int> IndexByName;
    std::vector<Stroka> NameByIndex;

};

////////////////////////////////////////////////////////////////////////////////

namespace NProto { class TNameTable; }
void ToProto(NProto::TNameTable* protoNameTable, const TNameTablePtr& nameTable);
TNameTablePtr FromProto(const NProto::TNameTable& protoNameTable);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
