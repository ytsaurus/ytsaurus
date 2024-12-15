#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TSnapshotLoadContext
{
    //! Reader used for reading snapshot content.
    NConcurrency::IAsyncZeroCopyInputStreamPtr Reader;

    //! Read-only mode from snapshot meta.
    bool ReadOnly;
};

////////////////////////////////////////////////////////////////////////////////

class TSnapshotLoadContextGuard
    : public TNonCopyable
{
public:
    explicit TSnapshotLoadContextGuard(TSnapshotLoadContext* context);
    ~TSnapshotLoadContextGuard();

private:
    TSnapshotLoadContext* Context_;
    TSnapshotLoadContext* SavedContext_;
};

////////////////////////////////////////////////////////////////////////////////

TSnapshotLoadContext* TryGetCurrentSnapshotLoadContext();
TSnapshotLoadContext* GetCurrentSnapshotLoadContext();
void SetCurrentSnapshotLoadContext(TSnapshotLoadContext* context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
