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

TSignatureGenerator::TSignatureGenerator(TSignatureGeneratorConfigPtr config, IKeyStoreWriterPtr keyWriter)
    : Config_(std::move(config))
    , KeyWriter_(std::move(keyWriter))
    , OwnerId_(KeyWriter_->GetOwner())
{
    InitializeCryptography();
    YT_LOG_INFO("Signature generator initialized (Owner: %v)", OwnerId_);
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TSignatureGenerator::Rotate()
{
    YT_LOG_INFO(
        "Rotating keypair (PreviousKeyPair: %v)",
        (KeyPair_ ? std::optional{GetKeyId(KeyPair_->KeyInfo()->Meta())} : std::nullopt));

    auto now = Now();
    TKeyPair newKeyPair(TKeyPairMetadataImpl<TKeyPairVersion{0, 1}>{
        .OwnerId = OwnerId_,
        .KeyId = TKeyId{TGuid::Create()},
        .CreatedAt = now,
        .ValidAfter = now - Config_->TimeSyncMargin,
        .ExpiresAt = now + Config_->KeyExpirationDelta,
    });

    return KeyWriter_->RegisterKey(newKeyPair.KeyInfo()).Apply(
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

void TSignatureGenerator::Sign(const TSignaturePtr& signature)
{
    auto signatureId = TGuid::Create();
    auto now = Now();
    TSignatureHeader header;

    {
        auto guard = ReaderGuard(KeyPairLock_);

        if (!KeyPair_) {
            THROW_ERROR_EXCEPTION("Trying to sign with an uninitialized generator");
        }

        header = TSignatureHeaderImpl<TSignatureVersion{0, 1}>{
            .Issuer = OwnerId_.Underlying(),
            .KeypairId = GetKeyId(KeyPair_->KeyInfo()->Meta()).Underlying(),
            .SignatureId = signatureId,
            .IssuedAt = now,
            .ValidAfter = now - Config_->TimeSyncMargin,
            .ExpiresAt = now + Config_->SignatureExpirationDelta,
        };

        signature->Header_ = ConvertToYsonString(header, EYsonFormat::Binary);

        auto toSign = PreprocessSignature(signature->Header_, signature->Payload());

        if (!IsKeyPairMetadataValid(KeyPair_->KeyInfo()->Meta())) {
            YT_LOG_WARNING(
                "Signing with an invalid keypair (SignatureId: %v, KeyPair: %v)",
                signatureId,
                GetKeyId(KeyPair_->KeyInfo()->Meta()));
        }

        signature->Signature_.resize(SignatureSize);
        KeyPair_->Sign(toSign, std::span<std::byte, SignatureSize>(signature->Signature_));
    }

    YT_LOG_TRACE(
        "Created signature (SignatureId: %v, Header: %v, Payload: %v)",
        signatureId,
        header,
        ConvertToYsonString(signature->Payload(), EYsonFormat::Text).ToString());

    YT_LOG_DEBUG(
        "Created signature (SignatureId: %v)",
        signatureId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
