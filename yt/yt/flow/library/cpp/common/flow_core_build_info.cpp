#include "flow_core_build_info.h"

#include <library/cpp/yt/logging/logger.h>

#include <build/scripts/c_templates/svnversion.h>

#include <util/charset/wide.h>
#include <util/string/builder.h>
#include <util/string/strip.h>

#include <string>
#include <string_view>

namespace NYT::NFlow {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

std::string ExtractScmField(std::string_view scmData, std::string_view key)
{
    for (size_t pos = 0; pos < scmData.size();) {
        auto lineEnd = scmData.find('\n', pos);
        auto line = StripString(scmData.substr(
            pos,
            lineEnd == std::string_view::npos ? scmData.size() - pos : lineEnd - pos));

        if (line.starts_with(key) && line.size() > key.size() && line[key.size()] == ':') {
            return std::string(StripString(line.substr(key.size() + 1)));
        }

        if (lineEnd == std::string_view::npos) {
            break;
        }
        pos = lineEnd + 1;
    }
    return {};
}

std::pair<std::string_view, bool> ExtractAsciiPrefix(std::string_view text)
{
    const char* first = text.data();
    const char* last = first + text.size();
    if (IsStringASCII(first, last)) {
        return {text, false};
    }
    // Fall back to a linear scan only on the (rare) mismatch path to
    // locate the first non-ASCII byte.
    for (const char* p = first; p != last; ++p) {
        if (static_cast<unsigned char>(*p) >= 0x80) {
            return {text.substr(0, p - first), true};
        }
    }
    Y_UNREACHABLE();
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

const TLogger Logger("FlowCoreVersion");

// Sentinel author value emitted by build/scripts/vcs_info.py when no
// real VCS info is available.
constexpr std::string_view UnknownAuthor = "<UNKNOWN>";

// Sentinel branch value emitted by build/scripts/vcs_info.py when no
// real VCS info is available.
constexpr std::string_view UnknownBranch = "unknown-vcs-branch";

// Sentinel commit summary emitted by build/scripts/vcs_info.py when no
// real VCS info is available.
constexpr std::string_view UnknownSummary = "No VCS";

// Sentinel build host value emitted by build/scripts/vcs_info.py when the
// real build host is unknown.
constexpr std::string_view UnknownBuildHost = "localhost";

// Returns true if |hash| is the all-zeros open-source no-VCS sentinel
// produced by build/scripts/vcs_info.py when no real revision is
// available. Empty input also returns true so callers can use a single
// "is missing" check.
bool IsMissingHash(const std::string& hash)
{
    return hash.empty() || hash.find_first_not_of('0') == std::string::npos;
}

TFlowCoreBuildInfoPtr ComputeFlowCoreBuildInfo()
{
    auto info = New<TFlowCoreBuildInfo>();

    std::string commitHash(GetProgramHash());
    if (!IsMissingHash(commitHash)) {
        info->CommitHash = std::move(commitHash);
    }

    std::string author(GetArcadiaLastAuthor());
    if (author != UnknownAuthor) {
        info->Author = std::move(author);
    }

    std::string branch(GetBranch());
    if (branch != UnknownBranch) {
        info->Branch = std::move(branch);
    }

    info->Tag = GetTag();

    auto summary = NDetail::ExtractScmField(GetProgramScmData(), "Summary");
    if (summary != UnknownSummary) {
        info->CommitSummary = std::move(summary);
    }

    // GetProgramSvnRevision() returns -1 (sentinel from vcs_info.py) or 0
    // (svnversion.h fallback) when not built from SVN-bridge trunk.
    if (auto revision = GetProgramSvnRevision(); revision > 0) {
        info->SvnRevision = revision;
    }

    std::string buildHost(GetProgramBuildHost());
    if (!buildHost.empty() && buildHost != UnknownBuildHost) {
        info->BuildHost = std::move(buildHost);
    }

    if (auto timestamp = GetProgramBuildTimestamp(); timestamp > 0) {
        info->BuildTimestamp = timestamp;
    }

    TStringBuilder logMessage;
    logMessage.AppendString("Flow core build info (");
    bool first = true;
    auto appendFieldIfNotEmpty = [&] (std::string_view name, std::string_view value) {
        if (value.empty()) {
            return;
        }
        if (!first) {
            logMessage.AppendString(", ");
        }
        first = false;
        logMessage.AppendFormat("%v: %v", name, value);
    };
    auto appendNumericFieldIfPositive = [&] (std::string_view name, std::string_view prefix, i64 value) {
        if (value <= 0) {
            return;
        }
        if (!first) {
            logMessage.AppendString(", ");
        }
        first = false;
        logMessage.AppendFormat("%v: %v%v", name, prefix, value);
    };
    appendFieldIfNotEmpty("CommitHash", info->CommitHash);
    appendFieldIfNotEmpty("Author", info->Author);
    appendFieldIfNotEmpty("Branch", info->Branch);
    appendFieldIfNotEmpty("Tag", info->Tag);
    appendFieldIfNotEmpty("CommitSummary", info->CommitSummary);
    appendNumericFieldIfPositive("SvnRevision", "r", info->SvnRevision);
    appendFieldIfNotEmpty("BuildHost", info->BuildHost);
    appendNumericFieldIfPositive("BuildTimestamp", "", info->BuildTimestamp);
    logMessage.AppendChar(')');
    YT_LOG_INFO("%v", logMessage);

    return info;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

void TFlowCoreBuildInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("commit_hash", &TThis::CommitHash)
        .Default();
    registrar.Parameter("author", &TThis::Author)
        .Default();
    registrar.Parameter("branch", &TThis::Branch)
        .Default();
    registrar.Parameter("tag", &TThis::Tag)
        .Default();
    registrar.Parameter("commit_summary", &TThis::CommitSummary)
        .Default();
    registrar.Parameter("svn_revision", &TThis::SvnRevision)
        .Default();
    registrar.Parameter("build_host", &TThis::BuildHost)
        .Default();
    registrar.Parameter("build_timestamp", &TThis::BuildTimestamp)
        .Default();
}

bool TFlowCoreBuildInfo::IsEmpty() const
{
    // SvnRevision <= 0 catches both our canonical absent sentinel (-1)
    // and the svnversion.h fallback value (0).
    return CommitHash.empty() && Author.empty() && Branch.empty() && Tag.empty() && CommitSummary.empty() && SvnRevision <= 0 && BuildHost.empty() && BuildTimestamp <= 0;
}

const TFlowCoreBuildInfoPtr& GetFlowCoreBuildInfo()
{
    static const TFlowCoreBuildInfoPtr cached = ComputeFlowCoreBuildInfo();
    return cached;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
