#pragma once

#include "public.h"

#include <yt/core/concurrency/rw_spinlock.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TMediumDirectory
    : public TRefCounted
{
public:
    Stroka GetNameByIndex(int index) const;
    int GetIndexByName(const Stroka& name) const;

    void UpdateDirectory(const NProto::TMediumDirectory& protoDirectory);

private:
    mutable NConcurrency::TReaderWriterSpinLock SpinLock_;
    yhash_map<Stroka, int> NameToIndex_;
    yhash_map<int, Stroka> IndexToName_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
