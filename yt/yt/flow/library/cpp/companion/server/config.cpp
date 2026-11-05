#include "config.h"

#include <yt/yt/core/ytree/convert.h>

#include <util/system/env.h>

namespace NYT::NFlow::NCompanionServer {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

NCompanion::TCompanionExecutionConfigPtr LoadCompanionExecutionConfigFromEnv()
{
    auto mode = GetEnv("YT_FLOW_MODE");
    THROW_ERROR_EXCEPTION_IF(mode.empty(),
        "YT_FLOW_MODE environment variable is not set; "
        "the companion binary must be spawned by a Flow worker");
    THROW_ERROR_EXCEPTION_UNLESS(mode == "Worker",
        "Companion process started in non-worker mode %Qv",
        mode);

    auto rawConfig = GetEnv("YT_FLOW_COMPANION_CONFIG");
    THROW_ERROR_EXCEPTION_IF(rawConfig.empty(),
        "YT_FLOW_COMPANION_CONFIG environment variable is not set");

    NCompanion::TCompanionExecutionConfigPtr config;
    try {
        config = ConvertTo<NCompanion::TCompanionExecutionConfigPtr>(
            NYson::TYsonStringBuf(rawConfig));
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to parse YT_FLOW_COMPANION_CONFIG")
            .With(ex);
    }

    THROW_ERROR_EXCEPTION_UNLESS(config->Port > 0,
        "YT_FLOW_COMPANION_CONFIG must specify a positive port, got %v",
        config->Port);
    // 0 means "auto" in the shared companion config vocabulary; the C++
    // companion resolves auto to a single multithreaded process.
    THROW_ERROR_EXCEPTION_UNLESS(config->CompanionProcessCount <= 1,
        "The C++ companion is single-process; companion_process_count must be 0 (auto) or 1, got %v",
        config->CompanionProcessCount);

    return config;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
