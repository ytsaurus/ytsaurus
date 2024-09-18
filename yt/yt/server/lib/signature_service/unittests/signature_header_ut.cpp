#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/lib/signature_service/signature_header.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NSignatureService {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace std::chrono_literals;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf HeaderString =
    R"({"version"="0.1";"issuer"="issuer";"keypair_id"="4-3-2-1";"signature_id"="8-7-6-5";"issued_at"="2024-08-09T14:44:55.000000Z";)"
    R"("expires_at"="2024-08-09T14:44:56.000000Z";"valid_after"="2024-08-09T14:44:54.000000Z";})";

////////////////////////////////////////////////////////////////////////////////

TEST(TSignatureHeader, SerializeDeserialize)
{
    TInstant instant;
    EXPECT_TRUE(TInstant::TryParseIso8601("2024-08-09T14:44:55Z", instant));

    TSignatureHeader header = TSignatureHeaderImpl<TSignatureVersion{0, 1}>{
        .Issuer="issuer",
        .KeypairId=TGuid{1, 2, 3, 4},
        .SignatureId=TGuid{5, 6, 7, 8},
        .IssuedAt=instant,
        .ValidAfter=instant - 1s,
        .ExpiresAt=instant + 1s,
    };

    EXPECT_EQ(
        ConvertToYsonString(header, EYsonFormat::Text).ToString(),
        HeaderString);

    EXPECT_EQ(ConvertTo<TSignatureHeader>(TYsonString(HeaderString)), header);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSignatureHeader, DeserializeErrors)
{
    EXPECT_THROW_WITH_SUBSTRING(
        ConvertTo<TSignatureHeader>(TYsonString("[]"_sb)),
        "node has invalid type");

    {
        auto headerNode = ConvertToNode(TYsonString(HeaderString));
        headerNode->AsMap()->GetChildOrThrow("version")->AsString()->SetValue("0.999");
        EXPECT_THROW_WITH_SUBSTRING(
            ConvertTo<TSignatureHeader>(headerNode),
            "Unknown TSignature version");
    }

    {
        auto headerNode = ConvertToNode(TYsonString(HeaderString));
        headerNode->AsMap()->RemoveChild("issuer");
        headerNode->AsMap()->AddChild("issuar", ConvertToNode(TYsonString("issuar"_sb)));
        EXPECT_THROW_WITH_SUBSTRING(
            ConvertTo<TSignatureHeader>(headerNode),
            "no child with key \"issuer\"");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSignatureService
