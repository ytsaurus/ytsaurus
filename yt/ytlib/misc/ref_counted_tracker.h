#pragma once

#include "common.h"

#include <util/stream/str.h>
#include <util/autoarray.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// Reference tracking relies on uniqueness of std::type_info objects.
// Without uniqueness reference tracking is still functional but lacks precision
// (i. e. some types may have duplicate entries in the accumulated table).
// GCC guarantees std::type_info uniqueness starting from version 3.0
// due to the so-called vague linking.
//
// See also: http://gcc.gnu.org/faq.html#dso
// See also: http://www.codesourcery.com/public/cxx-abi/

class TRefCountedTracker
    : private TNonCopyable
{
private:
    typedef const std::type_info* TKey;
    struct TItem
    {
        TKey Key;
        TAtomic AliveObjects;
        TAtomic TotalObjects;

        TItem()
            : Key(NULL)
            , AliveObjects(0)
            , TotalObjects(0)
        { }
    };

    static const int HashTableSize = 1009; // 1009 is a prime number

    typedef autoarray<TItem> TStatisticsMap;

    // search for key in Table
    // inserts under spinlock if not found
    TItem* Lookup(TKey key)
    {
        ui32 hash = THash<TKey>()(key) % HashTableSize;
        TItem* begin = Table.begin();
        TItem* current = begin + hash;
        TItem* end = Table.end();

        // iterate until find an appropriate cell
        for (;;) {
            if (current->Key == key) {
                return current;
            }
            if (EXPECT_FALSE(current->Key == NULL)) {
                current->Key = key;
                return current;
            }
            ++current;
            if (EXPECT_FALSE(current == end)) {
                current = begin;
            }
        }
    }

    // Comaperers
    struct TByAliveObjects
    {
        inline bool operator()(
            const TItem& lhs,
            const TItem& rhs) const
        {
            return lhs.AliveObjects > rhs.AliveObjects;
        }
    };

    struct TByTotalObjects
    {
        inline bool operator()(
            const TItem& lhs,
            const TItem& rhs) const
        {
            return lhs.TotalObjects > rhs.TotalObjects;
        }
    };

    struct TByName
    {
        inline bool operator()(
            const TItem& lhs,
            const TItem& rhs) const
        {
            return TCharTraits<char>::Compare(lhs.Key->name(), rhs.Key->name()) < 0;
        }
    };

private:
    TSpinLock SpinLock;
    TStatisticsMap Table;

public:
    TRefCountedTracker()
        : Table(HashTableSize)
    { }

    static TRefCountedTracker* Get()
    {
        return Singleton<TRefCountedTracker>();
    }

    void Increment(TKey typeInfo)
    {
        TItem* it;
        it = Lookup(typeInfo);

        if (EXPECT_FALSE(it == NULL)) {
            TGuard<TSpinLock> guard(SpinLock);
            it = Lookup(typeInfo);
        }

        ++(it->AliveObjects);
        ++(it->TotalObjects);
    }

    void Decrement(TKey typeInfo)
    {
        TItem* it = Lookup(typeInfo);

        --(it->AliveObjects);
    }

public:
    unsigned int GetAliveObjects(const std::type_info& typeInfo);
    unsigned int GetTotalObjects(const std::type_info& typeInfo);

    yvector<TItem> GetItems();
    Stroka GetDebugInfo(int sortByColumn = -1);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

