#include "debug_build_warning.h"

#include <library/cpp/build_info/build_info.h>

#include <util/string/ascii.h>
#include <util/string/type.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

// Build-type display names known to be significantly slower than release.
// Mirrors `ya make --build=` options (release, relwithdebinfo, minsizerel, profile,
// gprof, debugnoasserts, fastdebug stay silent) plus the sanitizer display names that
// CurrentBuildTypeDisplayName surfaces in place of the build type.
constexpr TStringBuf SlowBuildTypes[] = {
    "debug",
    "valgrind",
    "valgrind-release",
    "coverage",
    "ASAN",
    "TSAN",
    "MSAN",
    "UBSAN",
};

// Shared between the startup warning and the pipeline UI message so the wording stays in sync.
constexpr TStringBuf SlowerThanReleaseClause = "which is significantly slower than release";
constexpr TStringBuf RebuildCommandHint = "`ya make -r` (or relwithdebinfo, profile, etc.)";

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool IsSlowBuildType(TStringBuf buildType)
{
    for (auto slow : SlowBuildTypes) {
        if (AsciiEqualsIgnoreCase(buildType, slow)) {
            return true;
        }
    }
    return false;
}

bool IsSlowBuildWarningSuppressed(TStringBuf envValue)
{
    return IsTrue(envValue);
}

TString SlowBuildWarningMessage(TStringBuf reason)
{
    return TString::Join(
        "YT Flow build type is ",
        reason,
        ", ",
        SlowerThanReleaseClause,
        ".\n",
        "Rebuild with ",
        RebuildCommandHint,
        " for performance/production runs.\n",
        "Set ",
        SlowBuildSuppressEnvVarName,
        "=1 to silence this warning.");
}

TStringBuf SlowBuildSanitizerReason()
{
#if defined(_asan_enabled_)
    return "ASAN";
#elif defined(_tsan_enabled_)
    return "TSAN";
#elif defined(_msan_enabled_)
    return "MSAN";
#elif defined(_ubsan_enabled_)
    return "UBSAN";
#else
    return {};
#endif
}

std::string CurrentBuildTypeDisplayName()
{
    auto sanitizer = SlowBuildSanitizerReason();
    if (!sanitizer.empty()) {
        return std::string(sanitizer);
    }
    return std::string(GetBuildType());
}

std::string SlowBuildPipelineMessageText(TStringBuf buildType)
{
    return std::string("The controller build type is `") +
        std::string(buildType) +
        "`, " + std::string(SlowerThanReleaseClause) + ". " +
        "Rebuild the controller with " + std::string(RebuildCommandHint) +
        " for performance/production pipelines.";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
