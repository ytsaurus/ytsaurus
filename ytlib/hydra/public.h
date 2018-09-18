#pragma once

#include <yt/client/hydra/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TSnapshotMeta;
class TChangelogMeta;

} // namespace NProto

DECLARE_REFCOUNTED_CLASS(TPeerConnectionConfig)
DECLARE_REFCOUNTED_CLASS(TRemoteSnapshotStoreOptions)
DECLARE_REFCOUNTED_CLASS(TRemoteChangelogStoreOptions)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
