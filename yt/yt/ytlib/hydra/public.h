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

inline const std::string HeartbeatMutationType;
inline const std::string EnterReadOnlyMutationType = "NYT.NHydra.NProto.TReqEnterReadOnly";
inline const std::string ExitReadOnlyMutationType = "NYT.NHydra.NProto.TReqExitReadOnly";

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
