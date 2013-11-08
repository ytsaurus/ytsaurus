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
    int RegisterName(const TStringBuf& name);
    int GetOrRegisterName(const TStringBuf& name);

    TNullable<int> FindId(const TStringBuf& name) const;
    int GetId(const TStringBuf& name) const;

    const Stroka& GetName(int id) const;

    int GetSize() const;

private:
    TSpinLock SpinLock;

    std::vector<Stroka> IdToName;
    yhash_map<TStringBuf, int> NameToId; // String values are owned by IdToName.

    int DoRegisterName(const TStringBuf& name);

};

////////////////////////////////////////////////////////////////////////////////

namespace NProto { class TNameTableExt; }
void ToProto(NProto::TNameTableExt* protoNameTable, const TNameTablePtr& nameTable);
void FromProto(TNameTablePtr* nameTable, const NProto::TNameTableExt& protoNameTable);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
