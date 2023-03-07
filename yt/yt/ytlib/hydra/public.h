#pragma once

#include <yt/client/hydra/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TSnapshotMeta;

} // namespace NProto

DECLARE_REFCOUNTED_CLASS(TPeerConnectionConfig)
DECLARE_REFCOUNTED_CLASS(TRemoteSnapshotStoreOptions)
DECLARE_REFCOUNTED_CLASS(TRemoteChangelogStoreOptions)

extern const TString HeartbeatMutationType;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
