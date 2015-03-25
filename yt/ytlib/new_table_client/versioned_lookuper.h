#pragma once

#include "public.h"

#include <core/misc/error.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TLookuperPerformanceCounters
    : public virtual TIntrinsicRefCounted
{
    std::atomic<i64> StaticChunkRowLookupCount = {0};
    std::atomic<i64> StaticChunkRowLookupTrueNegativeCount = {0};
    std::atomic<i64> StaticChunkRowLookupFalsePositiveCount = {0};
};

DEFINE_REFCOUNTED_TYPE(TLookuperPerformanceCounters)

////////////////////////////////////////////////////////////////////////////////

struct IVersionedLookuper
    : public virtual TRefCounted
{
    //! Runs an asynchronous lookup.
    /*!
     *  The result remains valid as long as the lookuper instance lives and
     *  no more #Lookup calls are made.
     */
    virtual TFutureHolder<TVersionedRow> Lookup(TKey key) = 0;
};

DEFINE_REFCOUNTED_TYPE(IVersionedLookuper)

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
