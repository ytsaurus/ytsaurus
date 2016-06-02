#include "linear_probe.h"

#include <util/system/types.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr int HashTableExpansionParameter = 2;
constexpr int ValueLog = 48;

TLinearProbeHashTable::TStamp StampFromEntry(TLinearProbeHashTable::TEntry entry)
{
    return entry >> ValueLog;
}

TLinearProbeHashTable::TValue ValueFromEntry(TLinearProbeHashTable::TEntry entry)
{
    return entry & ((1ULL << ValueLog) - 1);
}

TLinearProbeHashTable::TEntry MakeEntry(TLinearProbeHashTable::TStamp stamp, TLinearProbeHashTable::TValue value)
{
    return (static_cast<TLinearProbeHashTable::TEntry>(stamp) << ValueLog) | value;
}

ui64 IndexFromFingerprint(TFingerprint fingerprint)
{
    return fingerprint;
}

TLinearProbeHashTable::TStamp StampFromFingerprint(TFingerprint fingerprint)
{
    TLinearProbeHashTable::TStamp stamp = fingerprint;
    if (stamp == 0) {
        stamp = 1;
    }
    return stamp;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TLinearProbeHashTable::TLinearProbeHashTable(size_t maxElementCount)
    : HashTable_(maxElementCount * HashTableExpansionParameter)
{ }

bool TLinearProbeHashTable::Insert(TFingerprint fingerprint, TValue value)
{
    return Insert(IndexFromFingerprint(fingerprint), StampFromFingerprint(fingerprint), value);
}

void TLinearProbeHashTable::Find(TFingerprint fingerprint, SmallVectorImpl<TValue>* result) const
{
    Find(IndexFromFingerprint(fingerprint), StampFromFingerprint(fingerprint), result);
}

bool TLinearProbeHashTable::Insert(ui64 index, TStamp stamp, TValue value)
{
    YCHECK(stamp != 0);
    YCHECK((value >> ValueLog) == 0);

    ui64 wrappedIndex = index % HashTable_.size();
    auto entry = MakeEntry(stamp, value);
    for (int currentIndex = 0; currentIndex < HashTable_.size(); ++currentIndex) {
        auto tableEntry = HashTable_[wrappedIndex].load(std::memory_order_relaxed);
        auto tableStamp = StampFromEntry(tableEntry);

        if (tableStamp == 0) {
            auto success = HashTable_[wrappedIndex].compare_exchange_strong(
                tableEntry,
                entry,
                std::memory_order_release,
                std::memory_order_relaxed);
            if (success) {
                return true;
            }
        }

        ++wrappedIndex;
        if (wrappedIndex == HashTable_.size()) {
            wrappedIndex = 0;
        }
    }

    return false;
}

void TLinearProbeHashTable::Find(ui64 index, TStamp stamp, SmallVectorImpl<TValue>* result) const
{
    Y_ASSERT(stamp != 0);

    ui64 wrappedIndex = index % HashTable_.size();
    for (int currentIndex = 0; currentIndex < HashTable_.size(); ++currentIndex) {
        auto tableEntry = HashTable_[wrappedIndex].load(std::memory_order_relaxed);
        auto tableStamp = StampFromEntry(tableEntry);

        if (tableStamp == 0) {
            break;
        }
        if (tableStamp == stamp) {
            result->push_back(ValueFromEntry(tableEntry));
        }

        ++wrappedIndex;
        if (wrappedIndex == HashTable_.size()) {
            wrappedIndex = 0;
        }
    }
}

size_t TLinearProbeHashTable::GetByteSize() const
{
    return sizeof(std::atomic<TEntry>) * HashTable_.size();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

