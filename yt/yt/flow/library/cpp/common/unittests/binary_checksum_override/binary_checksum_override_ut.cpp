#include <yt/yt/flow/library/cpp/common/checksum.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

// The checksum is resolved once per process, so the overridden and the computed paths cannot be
// covered by the same test module; this one runs with YT_FLOW_BINARY_CHECKSUM_OVERRIDE set.
TEST(TBinaryChecksumOverrideTest, EnvironmentValueIsUsedInsteadOfHashingTheBinary)
{
    EXPECT_EQ(GetBinaryChecksum(), "overridden-for-test");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
