#include "flow_core_version.h"

#include "checksum.h"
#include "flow_core_build_info.h"

#include <library/cpp/yt/logging/logger.h>

#include <util/generic/string.h>

namespace NYT::NFlow {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

const TLogger Logger("FlowCoreVersion");

std::string ComputeFlowCoreVersion()
{
    const auto& commit = GetFlowCoreBuildInfo()->CommitHash;
    if (!commit.empty()) {
        auto value = Format("%v (commit hash)", commit);
        YT_LOG_INFO("Resolved FlowCoreVersion from source control commit hash "
            "(FlowCoreVersion: %v)",
            value);
        return value;
    }

    auto checksum = GetBinaryChecksum();
    auto value = Format("%v (binary checksum)", checksum);
    YT_LOG_INFO("Source control commit hash is unavailable (likely a local, "
        "distbuild --no-vcs-info, or tarball build outside a repo); "
        "falling back to binary checksum "
        "(FlowCoreVersion: %v)",
        value);
    return value;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

const std::string& ResolveFlowCoreVersion()
{
    static const std::string cached = ComputeFlowCoreVersion();
    return cached;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
