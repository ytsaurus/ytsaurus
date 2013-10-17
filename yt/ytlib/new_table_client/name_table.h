#pragma once

#include "public.h"

#include <core/misc/nullable.h>

#include <ytlib/new_table_client/chunk_meta.pb.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class TNameTable
    : public virtual TRefCounted
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

TNameTablePtr FromProto(const NProto::TNameTable& protoNameTable);
void ToProto(NProto::TNameTable* protoNameTable, TNameTablePtr nameTable);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
