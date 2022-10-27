#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/crypto/crypto.h>

#include <limits>

namespace NYT::NCrypto {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TSha1Test, Simple)
{
    EXPECT_EQ("da39a3ee5e6b4b0d3255bfef95601890afd80709", TSha1Hasher().GetHexDigestLowerCase());
    EXPECT_EQ("da39a3ee5e6b4b0d3255bfef95601890afd80709", TSha1Hasher().Append("").GetHexDigestLowerCase());

    EXPECT_EQ("a9993e364706816aba3e25717850c26c9cd0d89d", TSha1Hasher().Append("abc").GetHexDigestLowerCase());
}

TEST(TSha256Test, Simple)
{
    EXPECT_EQ(
        "2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b",
        GetSha256HexDigestLowerCase("secret"));
    EXPECT_EQ(
        "2BB80D537B1DA3E38BD30361AA855686BDE0EACD7162FEF6A25FE97BF527A25B",
        GetSha256HexDigestUpperCase("secret"));
    EXPECT_EQ(
        "bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721",
        TSha256Hasher().Append("abc").Append("def").GetHexDigestLowerCase());
}

TEST(TMD5Test, Simple)
{
    EXPECT_EQ("d41d8cd98f00b204e9800998ecf8427e", TMD5Hasher().GetHexDigestLowerCase());
    EXPECT_EQ("d41d8cd98f00b204e9800998ecf8427e", TMD5Hasher().Append("").GetHexDigestLowerCase());

    EXPECT_EQ("900150983cd24fb0d6963f7d28e17f72", TMD5Hasher().Append("abc").GetHexDigestLowerCase());

    auto state = TMD5Hasher().Append("abacaba").GetState();
    TMD5State md5State = {
        1, 35, 69, 103, -119, -85, -51, -17, -2, -36, -70, -104, 118, 84, 50,
        16, 56, 0, 0, 0, 0, 0, 0, 0, 97, 98, 97, 99, 97, 98, 97, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0};
    EXPECT_EQ(state, md5State);
}

TEST(TEncryptPasswordTest, Simple)
{
    // Some canonical values.
    EXPECT_EQ(
        "5DE4BE05019FD8B24E74CC72756B091FF677C2C0B93F2827A6948B6A0018B958",
        EncryptPassword(/*password*/ "pass", /*salt*/ "salt"));
    EXPECT_EQ(
        "E9B20A4C9B200F7C7966448892B37BCCF26CB671CF6B23918CE4554C6EB76608",
        EncryptPassword(/*password*/ "pass", /*salt*/ "another_salt"));
    EXPECT_EQ(
        "7A336C6C16A6511B88732B7D2AF9F1E3DBF80F50B135544CE19F59E06AAAF39F",
        EncryptPassword(/*password*/ "another_pass", /*salt*/ "salt"));
}

TEST(TRngTest, Simple)
{
    auto firstString = GenerateCryptoStrongRandomString(32);
    EXPECT_EQ(32u, firstString.size());

    auto secondString = GenerateCryptoStrongRandomString(32);
    EXPECT_EQ(32u, secondString.size());

    EXPECT_FALSE(firstString == secondString);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCrypto

