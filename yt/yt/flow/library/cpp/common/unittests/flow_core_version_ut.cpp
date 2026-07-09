#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/checksum.h>
#include <yt/yt/flow/library/cpp/common/flow_core_build_info.h>
#include <yt/yt/flow/library/cpp/common/flow_core_version.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TFlowCoreVersionTest, IsCachedAndStable)
{
    // The resolved value must be a process-lifetime singleton: same
    // reference on every call.
    const auto& first = ResolveFlowCoreVersion();
    const auto& second = ResolveFlowCoreVersion();
    EXPECT_EQ(&first, &second);
    EXPECT_EQ(first, second);
}

TEST(TFlowCoreVersionTest, IsSuffixedWithSource)
{
    const auto& value = ResolveFlowCoreVersion();
    const bool hasCommitHashSuffix = value.ends_with(" (commit hash)");
    const bool hasBinaryChecksumSuffix = value.ends_with(" (binary checksum)");
    EXPECT_TRUE(hasCommitHashSuffix || hasBinaryChecksumSuffix)
        << "Resolved value must encode its source as a suffix; got: " << value;
    EXPECT_FALSE(hasCommitHashSuffix && hasBinaryChecksumSuffix);
}

TEST(TFlowCoreVersionTest, SourceMatchesBuildEnvironment)
{
    const auto& value = ResolveFlowCoreVersion();
    const auto& commit = GetFlowCoreBuildInfo()->CommitHash;

    if (commit.empty()) {
        // No VCS info baked in => must fall back to binary checksum.
        EXPECT_TRUE(value.ends_with(" (binary checksum)")) << value;
        EXPECT_EQ(value, GetBinaryChecksum() + " (binary checksum)");
    } else {
        // VCS info present => must use it.
        EXPECT_TRUE(value.ends_with(" (commit hash)")) << value;
        EXPECT_EQ(value, commit + " (commit hash)");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
