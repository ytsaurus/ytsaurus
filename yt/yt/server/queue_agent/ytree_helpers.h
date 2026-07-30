#pragma once

#include "private.h"

#include <yt/yt/core/ytree/public.h>
#include <yt/yt/core/ytree/virtual.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

//! A part of a merged virtual map, exposing GetSize/GetKeys/FindItemService publicly.
//! FindItemService must return null (not throw) for an absent key.
class TVirtualMapPartBase
    : public NYTree::TVirtualMapBase
{
public:
    using TVirtualMapBase::GetSize;
    using TVirtualMapBase::GetKeys;
    using TVirtualMapBase::FindItemService;
};

DEFINE_REFCOUNTED_TYPE(TVirtualMapPartBase)

//! Creates a virtual map service serving the union of the parts (disjoint keys, not deduplicated).
//! A lookup returns the first matching part; a part's throw propagates fail-fast.
NYTree::IYPathServicePtr CreateMergedVirtualMapService(std::vector<TVirtualMapPartBasePtr> parts);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
