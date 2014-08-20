#pragma once

#include "public.h"

#include <core/misc/error.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IVersionedLookuper
    : public virtual TRefCounted
{
    //! Runs an asynchronous lookup.
    /*!
     *  The result remains valid as long as the lookuper instance lives and
     *  no more #Lookup calls are made.
     */
    virtual TFuture<TErrorOr<TVersionedRow>> Lookup(TKey key) = 0;
};

DEFINE_REFCOUNTED_TYPE(IVersionedLookuper)

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
