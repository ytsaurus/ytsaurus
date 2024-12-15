#include "snapshot_load_context.h"

#include <yt/yt/core/concurrency/fls.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

TSnapshotLoadContextGuard::TSnapshotLoadContextGuard(TSnapshotLoadContext* context)
    : Context_(context)
    , SavedContext_(TryGetCurrentSnapshotLoadContext())
{
    SetCurrentSnapshotLoadContext(Context_);
}

TSnapshotLoadContextGuard::~TSnapshotLoadContextGuard()
{
    YT_ASSERT(TryGetCurrentSnapshotLoadContext() == Context_);
    SetCurrentSnapshotLoadContext(SavedContext_);
}

////////////////////////////////////////////////////////////////////////////////

static NConcurrency::TFlsSlot<TSnapshotLoadContext*> CurrentSnapshotLoadContextSlot;

TSnapshotLoadContext* TryGetCurrentSnapshotLoadContext()
{
    return *CurrentSnapshotLoadContextSlot;
}

TSnapshotLoadContext* GetCurrentSnapshotLoadContext()
{
    auto* snapshotLoadContext = TryGetCurrentSnapshotLoadContext();
    YT_ASSERT(snapshotLoadContext);
    return snapshotLoadContext;
}

void SetCurrentSnapshotLoadContext(TSnapshotLoadContext* context)
{
    *CurrentSnapshotLoadContextSlot = context;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
