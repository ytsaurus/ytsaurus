#include <yt/yt/core/test_framework/framework.h>

#include "stub_keystore.h"

#include <yt/yt/server/lib/signature/signature_validator.h>

#include <yt/yt/server/lib/signature/config.h>
#include <yt/yt/server/lib/signature/key_pair.h>
#include <yt/yt/server/lib/signature/signature_header.h>
#include <yt/yt/server/lib/signature/signature_preprocess.h>

#include <yt/yt/client/signature/signature.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/iterator/enumerate.h>

namespace NYT::NSignature {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace std::chrono_literals;
using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

struct TSignatureValidatorTest
    : public ::testing::Test
{
    TStubKeyStorePtr Store;
    TKeyId KeyId;
    TKeyPair Key;
    TSignatureValidatorConfigPtr Config;
    TSignatureValidatorPtr Validator;
    TYsonString Payload = TYsonString("MyImportantData"_sb);

    TSignatureValidatorTest()
        : Store(New<TStubKeyStore>())
        , KeyId(TGuid::Create())
        , Key(TKeyPairMetadataImpl<TKeyPairVersion{0, 1}>{
            .OwnerId = Store->OwnerId,
            .KeyId = KeyId,
            .CreatedAt = Now(),
            .ValidAfter = Now() - 10h,
            .ExpiresAt = Now() + 10h})
        , Config(New<TSignatureValidatorConfig>())
        , Validator(New<TSignatureValidator>(Config, Store))
    { }

    TSignatureHeader SimpleHeader(
        TDuration delta_created,
        TDuration delta_valid,
        TDuration delta_expires)
    {
        auto now = Now();
        return TSignatureHeaderImpl<TSignatureVersion{0, 1}>{
            .Issuer = Store->OwnerId.Underlying(),
            .KeypairId = KeyId.Underlying(),
            .SignatureId = TGuid::Create(),
            .IssuedAt = now + delta_created,
            .ValidAfter = now + delta_valid,
            .ExpiresAt = now + delta_expires
        };
    }

    bool RunValidate(const TSignaturePtr& signature)
    {
        return WaitFor(Validator->Validate(signature))
            .ValueOrThrow();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSignatureValidatorTest, ValidateGoodSignature)
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
        TSignaturePtr signature;
        EXPECT_NO_THROW(signature = ConvertTo<TSignaturePtr>(signatureString));

        EXPECT_FALSE(RunValidate(signature));

        WaitFor(Store->RegisterKey(Key.KeyInfo())).ThrowOnError();
        validateResults.push_back(RunValidate(signature));
    }

    EXPECT_EQ(validateResults, (std::vector{true, false, false}));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSignatureValidatorTest, ValidateBadSignature)
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
    TSignaturePtr signature;
    EXPECT_NO_THROW(signature = ConvertTo<TSignaturePtr>(signatureString));

    EXPECT_FALSE(RunValidate(signature));
    WaitFor(Store->RegisterKey(Key.KeyInfo())).ThrowOnError();
    EXPECT_FALSE(RunValidate(signature));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSignatureValidatorTest, ValidateInvalidSignature)
{
    TStringStream ss;
    TYsonWriter writer(&ss, EYsonFormat::Text);

    {
        writer.OnBeginMap();
        BuildYsonMapFragmentFluently(&writer)
            .Item("header").Value("abacaba")
            .Item("payload").Value(Payload.ToString())
            .Item("signature").Value(TString(SignatureSize, 'a'));
        writer.OnEndMap();
        writer.Flush();

        // Invalid header.
        EXPECT_FALSE(RunValidate(ConvertTo<TSignaturePtr>(TYsonString(ss.Str()))));
        ss.Clear();
    }

    {
        writer.OnBeginMap();
        BuildYsonMapFragmentFluently(&writer)
            .Item("header").Value("abacaba")
            .Item("payload").Value(Payload.ToString())
            .Item("signature").Value("123456789");
        writer.OnEndMap();
        writer.Flush();

        // Invalid signature length.
        EXPECT_FALSE(RunValidate(ConvertTo<TSignaturePtr>(TYsonString(ss.Str()))));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSignature
