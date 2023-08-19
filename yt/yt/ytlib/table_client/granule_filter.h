#pragma once

#include "public.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Interface of a callback that checks whether it's possible to skip a granule when processing some query.
//! By a "granule" we mean any part of a table, i.e. a chunk or a part of a chunk.
//! It is used in unversioned chunk readers (more precisely, when creating reader factories).
struct IGranuleFilter
    : public TRefCounted
{
    virtual bool CanSkip(
        const TColumnarStatistics& granuleColumnarStatistics,
        const TNameTablePtr& granuleNameTable) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IGranuleFilter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
