#ifndef DYNAMIC_MEMORY_STORE_INL_H_
#error "Direct inclusion of this file is not allowed, include dynamic_memory_store.h"
#endif

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

TTimestamp TDynamicMemoryStore::TimestampFromRevision(ui32 revision) const
{
    return RevisionToTimestamp_[revision];
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
