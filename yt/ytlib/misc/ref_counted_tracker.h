#pragma once

#include "common.h"

#include "../ytree/ytree_fwd.h"

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
public:
    typedef const std::type_info* TKey;

private:
    struct TItem
    {
        TKey Key;
        TAtomic AliveObjects;
        TAtomic CreatedObjects;

        TItem(TKey key)
            : Key(key)
            , AliveObjects(0)
            , CreatedObjects(0)
        { }
    };

public:
    typedef TItem* TCookie;

    static TCookie Lookup(TKey key);

    static inline void Register(TCookie cookie)
    {
        AtomicIncrement(cookie->AliveObjects);
        AtomicIncrement(cookie->CreatedObjects);
    }

    static inline void Unregister(TCookie cookie)
    {
        AtomicDecrement(cookie->AliveObjects);
    }

    static Stroka GetDebugInfo(int sortByColumn = -1);
    // TODO: experiment
    static void GetDebugInfo(NYTree::IYsonConsumer* consumer, int sortByColumn = -1);
    static i64 GetAliveObjects(TKey key);
    static i64 GetCreatedObjects(TKey key);

private:
    static yvector<TItem> GetItems();
    static void SortItems(yvector<TItem>& items, int sortByColumn);

    typedef yhash_map<TKey, TItem> TStatistics; 
    static TSpinLock SpinLock;
    static TStatistics Statistics;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

