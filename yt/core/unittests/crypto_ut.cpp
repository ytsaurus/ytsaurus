#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/crypto.h>

#include <limits>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TSHA1Test, Simple)
{
    EXPECT_EQ("da39a3ee5e6b4b0d3255bfef95601890afd80709", TSHA1Hasher().HexDigestLower());
    EXPECT_EQ("da39a3ee5e6b4b0d3255bfef95601890afd80709", TSHA1Hasher().Append("").HexDigestLower());

    EXPECT_EQ("a9993e364706816aba3e25717850c26c9cd0d89d", TSHA1Hasher().Append("abc").HexDigestLower());
}

TEST(TMD5Test, Simple)
{
    EXPECT_EQ("d41d8cd98f00b204e9800998ecf8427e", TMD5Hasher().HexDigestLower());
    EXPECT_EQ("d41d8cd98f00b204e9800998ecf8427e", TMD5Hasher().Append("").HexDigestLower());

    EXPECT_EQ("900150983cd24fb0d6963f7d28e17f72", TMD5Hasher().Append("abc").HexDigestLower());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

