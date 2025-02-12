#include "signature_validator.h"

#include "key_info.h"
#include "key_store.h"
#include "private.h"
#include "signature_header.h"
#include "signature_preprocess.h"

#include <yt/yt/client/signature/signature.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TSignatureValidator::TSignatureValidator(TSignatureValidatorConfigPtr config, IKeyStoreReaderPtr keyReader)
    : Config_(std::move(config))
    , KeyReader_(std::move(keyReader))
{
    InitializeCryptography();
    YT_LOG_INFO("Signature validator initialized");
}

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TMetadataCheckVisitor
{
    bool operator()(
        const TSignatureHeaderImpl<TSignatureVersion{0, 1}>& header) const
    {
        auto now = Now();

        return header.ValidAfter <= now && now < header.ExpiresAt;
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

TFuture<bool> TSignatureValidator::Validate(const TSignaturePtr& signature)
{
    TSignatureHeader header;
    try {
        header = ConvertTo<TSignatureHeader>(signature->Header_);
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(
            ex,
            "Received invalid signature header (Header: %v)",
            signature->Header_.ToString());
        return FalseFuture;
    }

    auto signatureId = std::visit([] (auto&& header_) { return header_.SignatureId; }, header);
    auto [keyIssuer, keyId] = std::visit(
        [] (auto&& header_) { return std::pair{TOwnerId(header_.Issuer), TKeyId(header_.KeypairId)}; },
        header);

    return KeyReader_->FindKey(keyIssuer, keyId).Apply(
        BIND([
                keyIssuer = std::move(keyIssuer),
                keyId = std::move(keyId),
                signatureId = std::move(signatureId),
                header = std::move(header),
                signature = std::move(signature)
            ] (const TKeyInfoPtr& keyInfo) {
                if (!keyInfo) {
                    YT_LOG_WARNING(
                        "Key not found (SignatureId: %v, Issuer: %v, KeyPair: %v)",
                        signatureId,
                        keyIssuer,
                        keyId);
                    return false;
                }

                auto toSign = PreprocessSignature(signature->Header_, signature->Payload());

                if (signature->Signature_.size() != SignatureSize) {
                    YT_LOG_WARNING(
                        "Signature size mismatch (SignatureId: %v, ReceivedSize: %v, ExpectedSize: %v)",
                        signatureId,
                        signature->Signature_.size(),
                        SignatureSize);
                    return false;
                }
                std::span<const std::byte, SignatureSize> signatureSpan(signature->Signature_);
                if (!keyInfo->Verify(toSign, signatureSpan)) {
                    YT_LOG_WARNING("Cryptographic verification failed (SignatureId: %v)", signatureId);
                    return false;
                }

                if (!std::visit(TMetadataCheckVisitor{}, header)) {
                    YT_LOG_WARNING("Metadata check failed (SignatureId: %v)", signatureId);
                    return false;
                }

                YT_LOG_DEBUG("Successfully validated (SignatureId: %v)", signatureId);
                return true;
            }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
