#pragma once

#include <yt/yt/client/hydra/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TSnapshotMeta;
class TChangelogMeta;

} // namespace NProto

DECLARE_REFCOUNTED_STRUCT(TPeerConnectionConfig)
DECLARE_REFCOUNTED_STRUCT(TRemoteSnapshotStoreOptions)
DECLARE_REFCOUNTED_STRUCT(TRemoteChangelogStoreOptions)

inline const TString HeartbeatMutationType;
inline const TString EnterReadOnlyMutationType = "NYT.NHydra.NProto.TReqEnterReadOnly";
inline const TString ExitReadOnlyMutationType = "NYT.NHydra.NProto.TReqExitReadOnly";

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
