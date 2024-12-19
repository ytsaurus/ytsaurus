#include "signature_generator.h"

#include "config.h"
#include "key_info.h"
#include "key_store.h"
#include "private.h"
#include "signature_header.h"
#include "signature_preprocess.h"

#include <yt/yt/client/signature/signature.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NLogging;
using namespace NYson;
using namespace std::chrono_literals;

////////////////////////////////////////////////////////////////////////////////

TSignatureGenerator::TSignatureGenerator(TSignatureGeneratorConfigPtr config, IKeyStoreWriterPtr store)
    : Config_(std::move(config))
    , Store_(std::move(store))
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
        .ValidAfter = now - Config_->TimeSyncMargin,
        .ExpiresAt = now + Config_->KeyExpirationDelta,
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
            .ValidAfter = now - Config_->TimeSyncMargin,
            .ExpiresAt = now + Config_->SignatureExpirationDelta,
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

        result->Signature_.resize(SignatureSize);
        KeyPair_->Sign(toSign, std::span<std::byte, SignatureSize>(result->Signature_));
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
