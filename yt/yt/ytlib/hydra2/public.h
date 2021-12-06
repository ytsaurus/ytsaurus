#pragma once

#include <yt/yt/client/hydra/public.h>

namespace NYT::NHydra2 {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TSnapshotMeta;

} // namespace NProto

DECLARE_REFCOUNTED_CLASS(TPeerConnectionConfig)
DECLARE_REFCOUNTED_CLASS(TRemoteSnapshotStoreOptions)
DECLARE_REFCOUNTED_CLASS(TRemoteChangelogStoreOptions)

extern const TString HeartbeatMutationType;

////////////////////////////////////////////////////////////////////////////////

using NElection::TCellId;
using NElection::NullCellId;
using NElection::TPeerId;
using NElection::InvalidPeerId;
using NElection::TPeerPriority;
using NElection::TEpochId;

using NHydra::EErrorCode;
using NHydra::EPeerState;
using NHydra::EPeerKind;
using NHydra::TVersion;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2
