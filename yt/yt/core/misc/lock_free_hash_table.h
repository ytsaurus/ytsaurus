#pragma once

#include "public.h"
#include "atomic_ptr.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TLockFreeHashTable
{
public:
    //! 64-bit hash table entry contains 16-bit stamp and 48-bit value.
    using TStamp = ui16;
    using TEntry = ui64;
    using TValuePtr = TIntrusivePtr<T>;

    explicit TLockFreeHashTable(size_t maxElementCount);

    ~TLockFreeHashTable();

    size_t GetByteSize() const;

    template <class TCallback>
    void ForEach(TCallback callback);

    size_t GetLoadFactor();

    //! Updates if there is equal element. Called from one thread.
    //! Update can interfere with Insert, but CAS and Exchange (or store) do not interfere because Update
    //! modifies only present elements and Insert modifies only empty elements.
    bool Update(TFingerprint fingerprint, TValuePtr value);

    //! Inserts element. Called concurrently from multiple threads.
    bool Insert(TFingerprint fingerprint, TValuePtr value);

    template <class TKey>
    TIntrusivePtr<T> Find(TFingerprint fingerprint, const TKey& key);

private:
    const size_t Size_;
    std::unique_ptr<std::atomic<TEntry>[]> HashTable_;

    static constexpr int HashTableExpansionFactor = 2;
    static constexpr int ValueLog = 48;

    static TStamp StampFromEntry(TEntry entry);

    static T* ValueFromEntry(TEntry entry);

    static TEntry MakeEntry(TStamp stamp, T* value);

    static size_t IndexFromFingerprint(TFingerprint fingerprint);

    static TStamp StampFromFingerprint(TFingerprint fingerprint);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define LOCK_FREE_HASH_TABLE_INL_H_
#include "lock_free_hash_table-inl.h"
#undef LOCK_FREE_HASH_TABLE_INL_H_
