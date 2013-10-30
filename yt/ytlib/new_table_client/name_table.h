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
    int RegisterName(const TStringBuf& name);
    int GetOrRegisterName(const TStringBuf& name);

    TNullable<int> FindIndex(const TStringBuf& name) const;
    int GetIndex(const TStringBuf& name) const;

    const Stroka& GetName(int index) const;

    int GetNameCount() const;

private:
    TSpinLock SpinLock;

    yhash_map<TStringBuf, int> NameToIndex; // names are owned by IndexToName
    std::vector<Stroka> IndexToName;

    int DoRegisterName(const TStringBuf& name);

};

void ToProto(NProto::TNameTableExt* protoNameTable, TNameTablePtr nameTable);
void FromProto(TNameTablePtr* nameTable, const NProto::TNameTableExt& protoNameTable);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
