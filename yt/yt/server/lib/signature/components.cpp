#include "components.h"

#include "config.h"
#include "cypress_key_store.h"
#include "key_rotator.h"
#include "signature_generator.h"
#include "signature_validator.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/concurrency/action_queue.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi::NNative;
using namespace NConcurrency;

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
{
    if (config->Validation || config->Generation) {
        auto actionQueue = New<TActionQueue>("CryptoInit");
        auto invoker = actionQueue->GetInvoker();

        // NB: destroy actionQueue upon completing initialization.
        InitializeCryptographyFuture_ = InitializeCryptography(invoker)
            .Apply(BIND([actionQueue = std::move(actionQueue)] {}));
    }
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TSignatureComponents::StartRotation()
{
    if (KeyRotator_) {
        return InitializeCryptographyFuture_.Apply(
            BIND([weakThis = MakeWeak(this)] {
                if (auto self = weakThis.Lock()) {
                    return self->KeyRotator_->Start();
                }
                return VoidFuture;
            }));
    }
    return VoidFuture;
}

TFuture<void> TSignatureComponents::StopRotation()
{
    return KeyRotator_ ? KeyRotator_->Stop() : VoidFuture;
}

TFuture<void> TSignatureComponents::RotateOutOfBand()
{
    if (KeyRotator_) {
        return InitializeCryptographyFuture_.Apply(
            BIND([weakThis = MakeWeak(this)] {
                if (auto self = weakThis.Lock()) {
                    return self->KeyRotator_->Rotate();
                }
                return VoidFuture;
            }));
    }
    return VoidFuture;
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
