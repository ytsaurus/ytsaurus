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
    static TNameTablePtr FromSchema(const TTableSchema& schema, bool excludeComputedColumns = false);
    static TNameTablePtr FromKeyColumns(const TKeyColumns& keyColumns);

    int GetSize() const;

    TNullable<int> FindId(const TStringBuf& name) const;

    int GetId(const TStringBuf& name) const;
    int RegisterName(const TStringBuf& name);
    int GetIdOrRegisterName(const TStringBuf& name);

    const Stroka& GetName(int id) const;

private:
    TSpinLock SpinLock_;

    std::vector<Stroka> IdToName_;
    yhash_map<TStringBuf, int> NameToId_; // String values are owned by IdToName.

    int DoRegisterName(const TStringBuf& name);

};

DEFINE_REFCOUNTED_TYPE(TNameTable)

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TNameTableExt* protoNameTable, const TNameTablePtr& nameTable);
void FromProto(TNameTablePtr* nameTable, const NProto::TNameTableExt& protoNameTable);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
