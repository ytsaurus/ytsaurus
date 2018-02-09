#include "config.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void WarnForUnrecognizedOptions(
    const NLogging::TLogger& logger,
    const NYTree::TYsonSerializablePtr& config)
{
    const auto& Logger = logger;
    auto unrecognized = config->GetUnrecognizedRecursively();
    if (unrecognized && unrecognized->GetChildCount() > 0) {
        LOG_WARNING("Master config contains unrecognized options (Unrecognized: %v)",
            ConvertToYsonString(unrecognized, NYson::EYsonFormat::Text));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

