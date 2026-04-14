#include "private.h"

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

void THighThreadCountJobInfo::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, JobId);
    PHOENIX_REGISTER_FIELD(2, ThreadCount);
}

PHOENIX_DEFINE_TYPE(THighThreadCountJobInfo);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
