#pragma once

#include <ytlib/ytree/public.h>

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

public:
    static TRefCountedTracker* Get();

    TCookie GetCookie(TKey key);

    inline void Register(TCookie cookie)
    {
        AtomicIncrement(cookie->AliveObjects);
        AtomicIncrement(cookie->CreatedObjects);
    }

    inline void Unregister(TCookie cookie)
    {
        AtomicDecrement(cookie->AliveObjects);
    }

    Stroka GetDebugInfo(int sortByColumn = -1);
    void GetMonitoringInfo(NYTree::IYsonConsumer* consumer);

    i64 GetAliveObjects(TKey key);
    i64 GetCreatedObjects(TKey key);

private:
    std::vector<TItem> GetItems();
    void SortItems(std::vector<TItem>& items, int sortByColumn);

    typedef yhash_map<TKey, TItem> TStatistics; 
    TSpinLock SpinLock;
    TStatistics Statistics;

};

////////////////////////////////////////////////////////////////////////////////

void DumpRefCountedTracker(int sortByColumn = -1);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

template <>
struct TSingletonTraits<NYT::TRefCountedTracker> {
    enum {
        Priority = 1024
    };
};
