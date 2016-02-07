#pragma once

#include "small_vector.h"
#include "public.h"

#include <util/system/types.h>

#include <atomic>
#include <vector>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TLinearProbeHashTable
{
public:
    // 64-bit hash table entry contains 16-bit stamp and 48-bit value.
    using TStamp = ui16;
    using TValue = ui64;
    using TEntry = ui64;

    explicit TLinearProbeHashTable(size_t maxElementCount);

    bool Insert(TFingerprint fingerprint, TValue value);
    void Find(TFingerprint fingerprint, SmallVectorImpl<TValue>* result) const;

    size_t GetByteSize() const;

private:
    std::vector<std::atomic<TEntry>> HashTable_;

    bool Insert(ui64 index, TStamp stamp, TValue value);
    void Find(ui64 index, TStamp stamp, SmallVectorImpl<TValue>* result) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

