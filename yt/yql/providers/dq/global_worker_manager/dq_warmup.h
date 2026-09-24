#pragma once

#include <yt/yql/providers/dq/common/yql_dq_warmup.h>

#include <util/generic/map.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>

namespace NActors {
class TActorSystem;
} // namespace NActors

namespace NYql {

class ICoordinationHelper;

void UploadWarmupArtifactsToYt(
    NActors::TActorSystem* actorSystem,
    const TIntrusivePtr<ICoordinationHelper>& coordinator,
    const TVector<TResourceManagerOptions>& ytBackends,
    const TString& vanillaJobLite,
    const TMap<TString, TString>& udfsWithMd5);

} // namespace NYql
