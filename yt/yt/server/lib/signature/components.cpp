#include "components.h"

#include "config.h"
#include "cypress_key_store.h"
#include "key_rotator.h"
#include "signature_generator.h"
#include "signature_validator.h"

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

TSignatureComponents::TSignatureComponents(
    const TSignatureComponentsConfigPtr& config,
    IClientPtr client,
    IInvokerPtr rotateInvoker)
    : Client_(std::move(client))
    , RotateInvoker_(std::move(rotateInvoker))
    , CypressKeyReader_(config->Validation
        ? New<TCypressKeyReader>(config->Validation->CypressKeyReader, Client_)
        : nullptr)
    , UnderlyingValidator_(config->Validation
        ? New<TSignatureValidator>(CypressKeyReader_)
        : nullptr)
    , SignatureValidator_(
        UnderlyingValidator_ ? UnderlyingValidator_ : CreateAlwaysThrowingSignatureValidator())
    , CypressKeyWriter_(config->Generation
        ? New<TCypressKeyWriter>(config->Generation->CypressKeyWriter, Client_)
        : nullptr)
    , UnderlyingGenerator_(config->Generation
        ? New<TSignatureGenerator>(config->Generation->Generator)
        : nullptr)
    , KeyRotator_(config->Generation
        ? New<TKeyRotator>(config->Generation->KeyRotator, RotateInvoker_, CypressKeyWriter_, UnderlyingGenerator_)
        : nullptr)
    , SignatureGenerator_(
        UnderlyingGenerator_ ? UnderlyingGenerator_ : CreateAlwaysThrowingSignatureGenerator())
{ }

TFuture<void> TSignatureComponents::Initialize()
{
    CypressKeyWriterInitialization_ = CypressKeyWriter_ ? CypressKeyWriter_->Initialize() : VoidFuture;
    return CypressKeyWriterInitialization_;
}

TFuture<void> TSignatureComponents::StartRotation()
{
    YT_VERIFY(CypressKeyWriterInitialization_);
    return CypressKeyWriterInitialization_
        .Apply(BIND([this_ = MakeWeak(this)] {
            if (auto self = this_.Lock(); self && self->KeyRotator_) {
                return self->KeyRotator_->Start();
            }
            return VoidFuture;
        }));
}

TFuture<void> TSignatureComponents::StopRotation()
{
    return KeyRotator_ ? KeyRotator_->Stop() : VoidFuture;
}

////////////////////////////////////////////////////////////////////////////////

const ISignatureGeneratorPtr& TSignatureComponents::GetSignatureGenerator()
{
    return SignatureGenerator_;
}

const ISignatureValidatorPtr& TSignatureComponents::GetSignatureValidator()
{
    return SignatureValidator_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
