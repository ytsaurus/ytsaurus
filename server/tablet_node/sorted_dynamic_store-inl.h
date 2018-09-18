#pragma once
#ifndef SORTED_DYNAMIC_STORE_INL_H_
#error "Direct inclusion of this file is not allowed, include sorted_dynamic_store.h"
#endif

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

TTimestamp TSortedDynamicStore::TimestampFromRevision(ui32 revision) const
{
    return RevisionToTimestamp_[revision];
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
