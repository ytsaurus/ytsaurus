#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/lib/signature/signature_preprocess.h>

#include <yt/yt/server/lib/signature/signature_header.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NSignature {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(SignaturePreprocessTest, Basic)
{
    EXPECT_THROW_WITH_SUBSTRING(
        Y_UNUSED(PreprocessSignature(TYsonString("header"_sb), "payload")),
        "node has invalid type");

    auto headerString = ConvertToYsonString(TSignatureHeader(
        TSignatureHeaderImpl<TSignatureVersion{0, 1}>{
            .Issuer="test",
            .KeypairId=TGuid(),
            .SignatureId=TGuid(),
            .IssuedAt=Now(),
            .ValidAfter=Now(),
            .ExpiresAt=Now(),
        }));

    auto toSign = PreprocessSignature(
        headerString,
        std::string("payload\0payload"_sb));

    std::string expected = headerString.ToString() + "\0payload\0payload"_sb;

    EXPECT_EQ(toSign, expected);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSignature
