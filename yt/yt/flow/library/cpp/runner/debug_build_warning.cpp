#include "debug_build_warning.h"

#include <yt/yt/flow/library/cpp/misc/debug_build_warning.h>

#include <yt/yt/core/logging/log.h>

#include <util/stream/output.h>
#include <util/system/env.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

TString SlowBuildWarningMessageIfAny()
{
    if (IsSlowBuildWarningSuppressed(GetEnv(TString(SlowBuildSuppressEnvVarName)))) {
        return {};
    }
    auto buildType = CurrentBuildTypeDisplayName();
    if (!IsSlowBuildType(buildType)) {
        return {};
    }
    return SlowBuildWarningMessage(buildType);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void MaybeWarnSlowBuild()
{
    if (auto message = SlowBuildWarningMessageIfAny(); !message.empty()) {
        Cerr << "WARNING: " << message << Endl;
    }
}

void MaybeLogSlowBuild(const NLogging::TLogger& logger)
{
    if (auto message = SlowBuildWarningMessageIfAny(); !message.empty()) {
        const auto& Logger = logger;
        YT_TLOG_WARNING(message);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
