#include "signature_validator.h"

#include "signature.h"
#include "signature_preprocess.h"
#include "signature_header.h"

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NSignatureService {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TSignatureValidator::TSignatureValidator(IKeyStoreReader* store)
    : Store_(store)
    , Logger("SignatureValidator")
{
    InitializeCryptography();
    YT_LOG_INFO("Signature validator initialized");
}

////////////////////////////////////////////////////////////////////////////////

namespace {

struct MetadataCheckVisitor
{
    bool operator()(
        const TSignatureHeaderImpl<TSignatureVersion{0, 1}>& header) const noexcept
    {
        auto now = Now();

        return header.ValidAfter <= now && now < header.ExpiresAt;
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool TSignatureValidator::Validate(const TSignature& signature)
{
    TSignatureHeader header;
    try {
        header = ConvertTo<TSignatureHeader>(signature.Header_);
    } catch(const std::exception& ex) {
        YT_LOG_WARNING(
            "Received invalid signature header (Header: %v, Error: %v)",
            signature.Header_.ToString(),
            ex);
        return false;
    }

    auto signatureId = std::visit([] (auto&& header_) { return header_.SignatureId; }, header);
    auto [keyIssuer, keyId] = std::visit(
        [] (auto&& header_) { return std::pair{TOwnerId(header_.Issuer), TKeyId(header_.KeypairId)}; },
        header);

    auto key = Store_->GetKey(keyIssuer, keyId);
    if (!key) {
        YT_LOG_DEBUG(
            "Key not found (SignatureId=%v, Issuer=%v, KeyPair=%v)",
            signatureId,
            keyIssuer,
            keyId);
        return false;
    }

    auto toSign = PreprocessSignature(signature.Header_, signature.Payload());
    if (!key->Verify(toSign, signature.Signature_)) {
        YT_LOG_DEBUG("Cryptographic verification failed (SignatureId: %v)", signatureId);
        return false;
    }

    if (!std::visit(MetadataCheckVisitor{}, header)) {
        YT_LOG_DEBUG("Metadata check failed (SignatureId: %v)", signatureId);
        return false;
    }

    YT_LOG_TRACE("Successfully validated (SignatureId: %v)", signatureId);
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignatureService
