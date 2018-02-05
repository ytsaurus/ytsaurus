#pragma once

#include <yt/core/misc/ref_counted.h>
#include <yt/core/misc/intrusive_ptr.h>
#include <yt/core/misc/size_literals.h>

#include <yt/core/ypath/public.h>

namespace NYT {
namespace NSkynetManager {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t SkynetPieceSize = 4_MB;

struct TCluster;

DECLARE_REFCOUNTED_STRUCT(TBootstrap)

DECLARE_REFCOUNTED_CLASS(TClusterConnectionConfig)
DECLARE_REFCOUNTED_CLASS(TCypressCoordinationConfig)
DECLARE_REFCOUNTED_CLASS(TSkynetManagerConfig)
DECLARE_REFCOUNTED_CLASS(TTombstoneCacheConfig)

DECLARE_REFCOUNTED_CLASS(TSkynetManager)
DECLARE_REFCOUNTED_CLASS(TCypressSync)
DECLARE_REFCOUNTED_CLASS(TShareCache)
DECLARE_REFCOUNTED_CLASS(TShareInfo)

DECLARE_REFCOUNTED_STRUCT(ISkynetApi)

DECLARE_REFCOUNTED_STRUCT(IShareHost)

//! (Cluster, TablePath, TableRevision).
typedef std::tuple<TString, NYPath::TYPath, i64> TShareKey;

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
