#include "public.h"

#include <yt/core/concurrency/public.h>

namespace NYT::NClusterNode {

////////////////////////////////////////////////////////////////////////////////

void ValidateTabletCellSnapshot(
    TBootstrap* bootstrap,
    const NConcurrency::IAsyncZeroCopyInputStreamPtr& reader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
