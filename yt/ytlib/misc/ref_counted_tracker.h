#pragma once

#include "common.h"

#include <util/stream/str.h>

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
    struct TStatistics
    {
        unsigned int AliveObjects;
        unsigned int TotalObjects;

        TStatistics()
            : AliveObjects(0)
            , TotalObjects(0)
        { }
    };

    typedef yhash_map<const std::type_info*, TStatistics> TStatisticsMap;
    typedef yvector< TPair<const std::type_info*, TStatistics> > TStatisticsVector;

    struct TByAliveObjects
    {
        inline bool operator()(
            const TStatisticsVector::value_type& lhs,
            const TStatisticsVector::value_type& rhs) const
        {
            return lhs.Second().AliveObjects > rhs.Second().AliveObjects;
        }
    };

    struct TByTotalObjects
    {
        inline bool operator()(
            const TStatisticsVector::value_type& lhs,
            const TStatisticsVector::value_type& rhs) const
        {
            return lhs.Second().TotalObjects > rhs.Second().TotalObjects;
        }
    };

    struct TByName
    {
        inline bool operator()(
            const TStatisticsVector::value_type& lhs,
            const TStatisticsVector::value_type& rhs) const
        {
            return TCharTraits<char>::Compare(lhs.First()->name(), rhs.First()->name()) < 0;
        }
    };

private:
    TSpinLock SpinLock;
    TStatisticsMap Table;

public:
    static TRefCountedTracker* Get()
    {
        return Singleton<TRefCountedTracker>();
    }

    void Increment(const std::type_info* typeInfo)
    {
        TGuard<TSpinLock> guard(&SpinLock);
        TStatisticsMap::iterator it = Table.find(typeInfo);

        if (EXPECT_FALSE(it == Table.end())) {
            it = Table.insert(MakePair(typeInfo, TStatistics())).First();
        }

        ++(it->Second().AliveObjects);
        ++(it->Second().TotalObjects);
    }

    void Decrement(const std::type_info* typeInfo)
    {
        TGuard<TSpinLock> guard(&SpinLock);
        TStatisticsMap::iterator it = Table.find(typeInfo);

        YASSERT(it != Table.end());

        --(it->Second().AliveObjects);
    }

public:
    unsigned int GetAliveObjects(const std::type_info& typeInfo) const;
    unsigned int GetTotalObjects(const std::type_info& typeInfo) const;

    Stroka GetDebugInfo(int sortByColumn = -1) const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

