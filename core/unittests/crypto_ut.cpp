#include <yt/core/test_framework/framework.h>

#include <yt/core/crypto/crypto.h>

#include <limits>

namespace NYT {
namespace NCrypto {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TSHA1Test, Simple)
{
    EXPECT_EQ("da39a3ee5e6b4b0d3255bfef95601890afd80709", TSHA1Hasher().GetHexDigestLower());
    EXPECT_EQ("da39a3ee5e6b4b0d3255bfef95601890afd80709", TSHA1Hasher().Append("").GetHexDigestLower());

    EXPECT_EQ("a9993e364706816aba3e25717850c26c9cd0d89d", TSHA1Hasher().Append("abc").GetHexDigestLower());
}

TEST(TMD5Test, Simple)
{
    EXPECT_EQ("d41d8cd98f00b204e9800998ecf8427e", TMD5Hasher().GetHexDigestLower());
    EXPECT_EQ("d41d8cd98f00b204e9800998ecf8427e", TMD5Hasher().Append("").GetHexDigestLower());

    EXPECT_EQ("900150983cd24fb0d6963f7d28e17f72", TMD5Hasher().Append("abc").GetHexDigestLower());

    auto state = TMD5Hasher().Append("abacaba").GetState();
    TMD5State md5State = {
        1, 35, 69, 103, -119, -85, -51, -17, -2, -36, -70, -104, 118, 84, 50,
        16, 56, 0, 0, 0, 0, 0, 0, 0, 97, 98, 97, 99, 97, 98, 97, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0};
    EXPECT_EQ(state, md5State);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NCrypto
} // namespace NYT

