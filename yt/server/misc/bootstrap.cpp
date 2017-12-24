#include "bootstrap.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {

using namespace NLogging;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TBootstrapBase::TBootstrapBase(
    const TLogger& logger,
    const TYsonSerializablePtr& config)
    : Logger(logger)
{
    auto unrecognized = config->GetUnrecognizedRecursively();
    if (unrecognized && unrecognized->GetChildCount() > 0) {
        LOG_WARNING("Bootstrap config contains unrecognized options (Unrecognized: %v)",
            ConvertToYsonString(unrecognized, EYsonFormat::Text));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT