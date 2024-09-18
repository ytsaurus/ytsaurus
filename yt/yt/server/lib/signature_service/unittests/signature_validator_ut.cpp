#include <yt/yt/core/test_framework/framework.h>

#include "mock/key_store.h"

#include <yt/yt/server/lib/signature_service/signature_validator.h>

#include <yt/yt/server/lib/signature_service/signature.h>
#include <yt/yt/server/lib/signature_service/signature_header.h>
#include <yt/yt/server/lib/signature_service/signature_preprocess.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/iterator/enumerate.h>

namespace NYT::NSignatureService {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace std::chrono_literals;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

struct TSignatureValidatorFixture
    : public ::testing::Test
{
    TKeyPair Key;
    TMockKeyStore Store;
    TSignatureValidator Validator;
    TYsonString Payload;

    TSignatureValidatorFixture()
        : Key(TKeyPairMetadata{
            .Owner = TOwnerId{"TMockKeyStore"},
            .Id = TKeyId{TGuid::Create()},
            .CreatedAt = Now(),
            .ValidAfter = Now() - 10h,
            .ExpiresAt = Now() + 10h})
        , Store()
        , Validator(&Store)
        , Payload("MyImportantData"_sb)
    {
    }

    TSignatureHeader SimpleHeader(
        std::chrono::duration<double> delta_created,
        std::chrono::duration<double> delta_valid,
        std::chrono::duration<double> delta_expires)
    {
        auto now = Now();
        return TSignatureHeaderImpl<TSignatureVersion{0, 1}>{
            .Issuer = "TMockKeyStore",
            .KeypairId = Key.KeyInfo().Meta().Id.Underlying(),
            .SignatureId = TGuid::Create(),
            .IssuedAt = now + delta_created,
            .ValidAfter = now + delta_valid,
            .ExpiresAt = now + delta_expires
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSignatureValidatorFixture, ValidateGoodSignature)
{
    // Valid, not yet valid, expired.
    std::vector<TSignatureHeader> headers = {SimpleHeader(-10h, -10h, 10h), SimpleHeader(-10h, 5h, 10h), SimpleHeader(-10h, -10h, -5h)};
    std::vector<bool> validateResults;
    for (auto [i, header] : Enumerate(headers)) {
        auto headerString = ConvertToYsonString(header);
        auto toSign = PreprocessSignature(headerString, Payload);
        std::array<std::byte, SignatureSize> signatureBytes;
        Key.Sign(toSign, signatureBytes);

        TStringStream ss;
        TYsonWriter writer(&ss, EYsonFormat::Text);

        writer.OnBeginMap();
        BuildYsonMapFragmentFluently(&writer)
            .Item("header").Value(headerString.ToString())
            .Item("payload").Value(Payload.ToString())
            .Item("signature").Value(TString(
                reinterpret_cast<const char*>(signatureBytes.data()),
                signatureBytes.size()
            ));
        writer.OnEndMap();
        writer.Flush();

        TYsonString signatureString(ss.Str());
        TSignature signature;
        EXPECT_NO_THROW(signature = ConvertTo<TSignature>(signatureString));

        EXPECT_FALSE(Validator.Validate(signature));

        Store.RegisterKey(Key.KeyInfo());
        validateResults.push_back(Validator.Validate(signature));
    }

    EXPECT_EQ(validateResults, (std::vector{true, false, false}));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSignatureValidatorFixture, ValidateBadSignature)
{
    TYsonString headerString = ConvertToYsonString(SimpleHeader(-10h, -10h, 10h));

    TStringStream ss;
    TYsonWriter writer(&ss, EYsonFormat::Text);

    writer.OnBeginMap();
    BuildYsonMapFragmentFluently(&writer)
        .Item("header").Value(headerString.ToString())
        .Item("payload").Value(Payload.ToString())
        .Item("signature").Value(TString(SignatureSize, 'a'));
    writer.OnEndMap();
    writer.Flush();

    TYsonString signatureString(ss.Str());
    TSignature signature;
    EXPECT_NO_THROW(signature = ConvertTo<TSignature>(signatureString));

    EXPECT_FALSE(Validator.Validate(signature));
    Store.RegisterKey(Key.KeyInfo());
    EXPECT_FALSE(Validator.Validate(signature));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSignatureValidatorFixture, ValidateInvalidSignature)
{
    TSignature signature;
    TStringStream ss;
    TYsonWriter writer(&ss, EYsonFormat::Text);

    writer.OnBeginMap();
    BuildYsonMapFragmentFluently(&writer)
        .Item("header").Value("abacaba")
        .Item("payload").Value(Payload.ToString())
        .Item("signature").Value(TString(SignatureSize, 'a'));
    writer.OnEndMap();
    writer.Flush();

    // Invalid header.
    EXPECT_FALSE(Validator.Validate(ConvertTo<TSignature>(TYsonString(ss.Str()))));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSignatureService
