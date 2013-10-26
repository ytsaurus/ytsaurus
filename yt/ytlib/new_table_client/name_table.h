#pragma once

#include "public.h"

#include <core/misc/nullable.h>

#include <yt/ytlib/new_table_client/chunk_meta.pb.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class TNameTable
    : public virtual TRefCounted
{
public:
    int RegisterName(const Stroka& name);
    int GetOrRegisterName(const Stroka& name);

    TNullable<int> FindIndex(const Stroka& name) const;
    int GetIndex(const Stroka& name) const;

    const Stroka& GetName(int index) const;

    int GetNameCount() const;

private:
    TSpinLock SpinLock;

    yhash_map<Stroka, int> IndexByName;
    std::vector<Stroka> NameByIndex;

    int DoRegisterName(const Stroka& name);

};

void ToProto(NProto::TNameTableExt* protoNameTable, TNameTablePtr nameTable);
void FromProto(TNameTablePtr* nameTable, const NProto::TNameTableExt& protoNameTable);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
