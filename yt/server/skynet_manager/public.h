#pragma once

#include <yt/core/misc/intrusive_ptr.h>
#include <yt/core/misc/size_literals.h>

namespace NYT {
namespace NSkynetManager {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t SkynetPieceSize = 4_MB;

DECLARE_REFCOUNTED_CLASS(TBootstrap)

DECLARE_REFCOUNTED_CLASS(TClusterConnectionConfig)
DECLARE_REFCOUNTED_CLASS(TSkynetManagerConfig)

DECLARE_REFCOUNTED_CLASS(TSkynetManager)

DECLARE_REFCOUNTED_STRUCT(ISkynetApi)

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
