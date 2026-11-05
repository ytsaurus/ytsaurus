#include "private.h"

#include <yt/yt/ytlib/controller_agent/serialize.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

void THighThreadCountJobInfo::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, JobId);
    PHOENIX_REGISTER_FIELD(2, ThreadCount);
    PHOENIX_REGISTER_FIELD(3, Threshold,
        .SinceVersion(ESnapshotVersion::HighThreadCountJobThreshold));
}

PHOENIX_DEFINE_TYPE(THighThreadCountJobInfo);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
