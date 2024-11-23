#include "signature_generator.h"

#include "private.h"
#include "signature.h"
#include "signature_header.h"
#include "signature_preprocess.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NLogging;
using namespace NYson;
using namespace std::chrono_literals;

////////////////////////////////////////////////////////////////////////////////

TSignatureGenerator::TSignatureGenerator(const IKeyStoreWriterPtr& keyStore)
    : Store_(keyStore)
    , Owner_(Store_->GetOwner())
{
    InitializeCryptography();
    YT_LOG_INFO("Signature generator initialized (Owner: %v)", Owner_);
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TSignatureGenerator::Rotate()
{
    YT_LOG_INFO(
        "Rotating keypair (PreviousKeyPair: %v)",
        (KeyPair_ ? std::optional{GetKeyId(KeyPair_->KeyInfo()->Meta())} : std::nullopt));

    auto now = Now();
    TKeyPair newKeyPair(TKeyPairMetadataImpl<TKeyPairVersion{0, 1}>{
        .Owner = Owner_,
        .Id = TKeyId{TGuid::Create()},
        .CreatedAt = now,
        .ValidAfter = now - TimeSyncMargin,
        .ExpiresAt = now + KeyExpirationTime,
    });

    return Store_->RegisterKey(newKeyPair.KeyInfo()).Apply(
        BIND([this, keyPair = std::move(newKeyPair), this_ = MakeStrong(this)] () mutable {
            {
                auto guard = WriterGuard(KeyPairLock_);
                KeyPair_ = std::move(keyPair);
            }
            YT_LOG_INFO("Rotated keypair (NewKeyPair: %v)", GetKeyId(KeyPair_->KeyInfo()->Meta()));
        }));
}

////////////////////////////////////////////////////////////////////////////////

TKeyInfoPtr TSignatureGenerator::KeyInfo() const
{
    YT_VERIFY(KeyPair_);
    return KeyPair_->KeyInfo();
}

////////////////////////////////////////////////////////////////////////////////

TSignaturePtr TSignatureGenerator::Sign(TYsonString&& payload) const
{
    auto signatureId = TGuid::Create();
    auto now = Now();
    TSignaturePtr result = New<TSignature>();
    TSignatureHeader header;

    {
        auto guard = ReaderGuard(KeyPairLock_);

        if (!KeyPair_) {
            THROW_ERROR_EXCEPTION("Trying to sign with an uninitialized generator");
        }

        header = TSignatureHeaderImpl<TSignatureVersion{0, 1}>{
            .Issuer = Owner_.Underlying(),
            .KeypairId = GetKeyId(KeyPair_->KeyInfo()->Meta()).Underlying(),
            .SignatureId = signatureId,
            .IssuedAt = now,
            .ValidAfter = now - TimeSyncMargin,
            .ExpiresAt = now + SignatureExpirationTime,
        };

        result->Header_ = ConvertToYsonString(header, EYsonFormat::Binary);
        result->Payload_ = std::move(payload);

        auto toSign = PreprocessSignature(result->Header_, result->Payload_);

        if (!IsKeyPairMetadataValid(KeyPair_->KeyInfo()->Meta())) {
            YT_LOG_WARNING(
                "Signing with an invalid keypair (SignatureId: %v, KeyPair: %v)",
                signatureId,
                GetKeyId(KeyPair_->KeyInfo()->Meta()));
        }

        KeyPair_->Sign(toSign, result->Signature_);
    }

    YT_LOG_TRACE(
        "Created signature (SignatureId: %v, Header: %v, Payload: %v)",
        signatureId,
        header,
        ConvertToYsonString(result->Payload_, EYsonFormat::Text).ToString());

    YT_LOG_DEBUG(
        "Created signature (SignatureId: %v)",
        signatureId);

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
