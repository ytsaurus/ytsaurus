#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

#include <string>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Env var that silences the startup slow-build warning when set to a true value ("1").
//! Silences the stderr/log warning only — not the build type reported via node_info
//! or the pipeline-UI message.
constexpr TStringBuf SlowBuildSuppressEnvVarName = "YT_FLOW_SUPPRESS_DEBUG_BUILD_WARNING";

//! True iff the build-type display name is significantly slower than release
//! (case-insensitive: "DEBUG" == "debug"). Recognizes the slow `ya make --build=` options
//! (debug, valgrind, coverage) and the sanitizer display names that
//! CurrentBuildTypeDisplayName produces. Everything else (release, relwithdebinfo,
//! profile, ...) is performance-ok.
bool IsSlowBuildType(TStringBuf buildType);

//! True iff |envValue| (a SlowBuildSuppressEnvVarName value) silences the warning:
//! true values ("1", "true", ...) suppress; "", "0", "false" and garbage keep it.
bool IsSlowBuildWarningSuppressed(TStringBuf envValue);

//! Human-readable warning for a slow build with the given reason ("debug"/"ASAN"/...).
//! Targeted at stderr / process log (mentions the suppression env-var).
TString SlowBuildWarningMessage(TStringBuf reason);

//! Returns "ASAN"/"TSAN"/"MSAN"/"UBSAN" when the corresponding sanitizer is enabled at
//! compile time, "" otherwise. LSan-only builds stay silent: leak checking is cheap
//! enough at runtime.
TStringBuf SlowBuildSanitizerReason();

//! Build-type label reported in TNodeInfo and shown in the pipeline UI:
//! sanitizer name if active, otherwise GetBuildType().
std::string CurrentBuildTypeDisplayName();

//! Markdown text describing the slow build identified by |buildType| (a TNodeInfo build-type
//! display name, e.g. "debug" or "ASAN"), targeted at the pipeline UI (no env-var hint).
std::string SlowBuildPipelineMessageText(TStringBuf buildType);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
