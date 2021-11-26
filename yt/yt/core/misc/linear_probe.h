#pragma once

#include "compact_vector.h"
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

    template <size_t N>
    void Find(TFingerprint fingerprint, TCompactVector<TValue, N>* result) const;

    size_t GetByteSize() const;

private:
    constexpr static int HashTableExpansionParameter = 2;
    constexpr static int ValueLog = 48;

    std::vector<std::atomic<TEntry>> HashTable_;

    bool Insert(ui64 index, TStamp stamp, TValue value);

    template <size_t N>
    void Find(ui64 index, TStamp stamp, TCompactVector<TValue, N>* result) const;

    static TStamp StampFromEntry(TEntry entry);
    static TValue ValueFromEntry(TEntry entry);
    static TEntry MakeEntry(TStamp stamp, TValue value);

    static ui64 IndexFromFingerprint(TFingerprint fingerprint);
    static TStamp StampFromFingerprint(TFingerprint fingerprint);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define LINEAR_PROBE_INL_H_
#include "linear_probe-inl.h"
#undef LINEAR_PROBE_INL_H_
