#pragma once

#include <yt/core/misc/intrusive_ptr.h>
#include <yt/core/misc/size_literals.h>

#include <yt/core/ypath/public.h>

namespace NYT::NSkynetManager {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t SkynetPieceSize = 4_MB;
constexpr size_t MaxTableShards = 16 * 1024;
constexpr i64 MaxResourceSize = 1_TB;

typedef TString TResourceId;

struct TCluster;

typedef std::function<void(i64 current)>
    TProgressCallback;


DECLARE_REFCOUNTED_STRUCT(TBootstrap)

DECLARE_REFCOUNTED_CLASS(TClusterConnectionConfig)
DECLARE_REFCOUNTED_CLASS(TSkynetManagerConfig)

DECLARE_REFCOUNTED_CLASS(TPeerConnection)
DECLARE_REFCOUNTED_CLASS(TPeerListener)
DECLARE_REFCOUNTED_CLASS(TAnnouncer)
DECLARE_REFCOUNTED_CLASS(TAnnouncerConfig)

DECLARE_REFCOUNTED_CLASS(TTables)

DECLARE_REFCOUNTED_STRUCT(TResourceLink)

DECLARE_REFCOUNTED_CLASS(TShareOperation)
DECLARE_REFCOUNTED_CLASS(TClusterConnection)
DECLARE_REFCOUNTED_CLASS(TSkynetService)

DECLARE_REFCOUNTED_CLASS(TTrackerConnection)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSkynetManager
