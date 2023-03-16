#pragma once

#include "public.h"

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct THedgingUnit
{
    std::optional<TString> UserTag;
    bool HunkChunk;

    bool operator == (const THedgingUnit& other) const;

    explicit operator size_t() const;
};

////////////////////////////////////////////////////////////////////////////////

struct ITabletHedgingManagerRegistry
    : public TRefCounted
{
    //! Returns hedging manager for specific #hedgingUnit
    //! and creates one if not present.
    //! May return null in case of disabled hedging determined via config.
    virtual IHedgingManagerPtr GetOrCreateHedgingManager(const THedgingUnit& hedgingUnit) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITabletHedgingManagerRegistry)

////////////////////////////////////////////////////////////////////////////////

struct IHedgingManagerRegistry
    : public TRefCounted
{
    virtual ITabletHedgingManagerRegistryPtr GetOrCreateTabletHedgingManagerRegistry(
        NTableClient::TTableId tableId,
        const TAdaptiveHedgingManagerConfigPtr& storeChunkConfig,
        const TAdaptiveHedgingManagerConfigPtr& hunkChunkConfig,
        const NProfiling::TProfiler& profiler) = 0;
};

DEFINE_REFCOUNTED_TYPE(IHedgingManagerRegistry)

////////////////////////////////////////////////////////////////////////////////

// ! Uses #invoker for background eviction of expired tablet hedging manager registries.
IHedgingManagerRegistryPtr CreateHedgingManagerRegistry(IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT::NTabletNode
