#include "signature_generator.h"

#include "config.h"
#include "key_info.h"
#include "private.h"
#include "signature_header.h"
#include "signature_preprocess.h"

#include <yt/yt/client/signature/signature.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NLogging;
using namespace NYson;
using namespace std::chrono_literals;

////////////////////////////////////////////////////////////////////////////////

TSignatureGenerator::TSignatureGenerator(TSignatureGeneratorConfigPtr config)
    : Config_(std::move(config))
{
    YT_LOG_INFO("Signature generator initialized");
}

void TSignatureGenerator::Reconfigure(TSignatureGeneratorConfigPtr config)
{
    YT_VERIFY(config);
    Config_.Store(std::move(config));
    YT_LOG_INFO("Signature generator reconfigured");
}

////////////////////////////////////////////////////////////////////////////////

void TSignatureGenerator::SetKeyPair(TKeyPairPtr keyPair)
{
    bool isSane = keyPair->CheckSanity();
    auto keyId = GetKeyId(keyPair->KeyInfo()->Meta());

    YT_LOG_DEBUG("Setting new key pair (KeyId: %v)", keyId);
    YT_VERIFY(isSane);
    KeyPair_.Store(std::move(keyPair));
}

////////////////////////////////////////////////////////////////////////////////

TKeyInfoPtr TSignatureGenerator::KeyInfo() const
{
    return KeyPair_ ? KeyPair_.Acquire()->KeyInfo() : nullptr;
}

////////////////////////////////////////////////////////////////////////////////

void TSignatureGenerator::Resign(const TSignaturePtr& signature) const
{
    TKeyPairPtr signingKeyPair = KeyPair_.Acquire();
    if (!signingKeyPair) {
        THROW_ERROR_EXCEPTION(NRpc::EErrorCode::TransientFailure, "Signature generator is not ready yet");
    }

    auto keyInfo = signingKeyPair->KeyInfo();
    auto config = Config_.Acquire();
    auto signatureId = TGuid::Create();
    auto now = Now();
    TSignatureHeader header = TSignatureHeaderImpl<TSignatureVersion{0, 1}>{
        .Issuer = GetOwnerId(keyInfo->Meta()).Underlying(),
        .KeypairId = GetKeyId(keyInfo->Meta()).Underlying(),
        .SignatureId = signatureId,
        .IssuedAt = now,
        .ValidAfter = now - config->TimeSyncMargin,
        .ExpiresAt = now + config->SignatureExpirationDelta,
    };

    signature->Header_ = ConvertToYsonString(header, EYsonFormat::Binary);

    auto toSign = PreprocessSignature(signature->Header_, signature->Payload());

    if (!IsKeyPairMetadataValid(keyInfo->Meta())) {
        YT_LOG_WARNING(
            "Signing with an invalid keypair (SignatureId: %v, KeyPair: %v)",
            signatureId,
            GetKeyId(keyInfo->Meta()));
    }

    signature->Signature_.resize(SignatureSize);
    signingKeyPair->Sign(toSign, std::span<char, SignatureSize>(signature->Signature_));

    YT_LOG_TRACE(
        "Created signature (SignatureId: %v, Header: %v, Payload: %v)",
        signatureId,
        header,
        signature->Payload());

    YT_LOG_DEBUG(
        "Created signature (SignatureId: %v)",
        signatureId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
