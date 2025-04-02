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

TSignatureGenerator::TSignatureGenerator(TSignatureGeneratorConfigPtr config)
    : Config_(std::move(config))
{
    YT_LOG_INFO("Signature generator initialized");
}

////////////////////////////////////////////////////////////////////////////////

void TSignatureGenerator::SetKeyPair(TKeyPairPtr keyPair)
{
    auto keyId = GetKeyId(keyPair->KeyInfo()->Meta());
    YT_LOG_DEBUG("Setting new key pair (KeyId: %v)", keyId);

    YT_VERIFY(keyPair->CheckSanity());

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
    auto signatureId = TGuid::Create();
    auto now = Now();
    TSignatureHeader header;

    TKeyPairPtr signingKeyPair = KeyPair_.Acquire();

    if (!signingKeyPair) {
        THROW_ERROR_EXCEPTION("Trying to sign with an uninitialized generator");
    }

    auto keyInfo = signingKeyPair->KeyInfo();

    header = TSignatureHeaderImpl<TSignatureVersion{0, 1}>{
        .Issuer = GetOwnerId(keyInfo->Meta()).Underlying(),
        .KeypairId = GetKeyId(KeyInfo()->Meta()).Underlying(),
        .SignatureId = signatureId,
        .IssuedAt = now,
        .ValidAfter = now - Config_->TimeSyncMargin,
        .ExpiresAt = now + Config_->SignatureExpirationDelta,
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
