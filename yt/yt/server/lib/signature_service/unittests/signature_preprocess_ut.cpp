#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/lib/signature_service/signature_preprocess.h>

#include <yt/yt/server/lib/signature_service/signature_header.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NSignatureService {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(SignatureService, PreprocessSignature)
{
    EXPECT_THROW_WITH_SUBSTRING(
        std::ignore = PreprocessSignature(TYsonString("header"_sb), TYsonString("payload"_sb)),
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

    std::vector<std::byte> toSign = PreprocessSignature(
        headerString,
        TYsonString("payload\0payload"_sb));

    TString expected = headerString.ToString() + "\0payload\0payload"_sb;
    auto expectedBytes = std::as_bytes(std::span(TStringBuf(expected)));

    EXPECT_EQ(toSign, std::vector<std::byte>(expectedBytes.begin(), expectedBytes.end()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSignatureService
