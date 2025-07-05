#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/actions/public.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

class TSignatureComponents final
{
public:
    TSignatureComponents(
        const TSignatureInstanceConfigPtr& config,
        NApi::IClientPtr client,
        IInvokerPtr rotateInvoker);

    TFuture<void> Initialize();

    TFuture<void> StartRotation();

    TFuture<void> StopRotation();

    const ISignatureValidatorPtr& GetSignatureValidator();
    const ISignatureGeneratorPtr& GetSignatureGenerator();

private:
    const NApi::IClientPtr Client_;
    const IInvokerPtr RotateInvoker_;
    TCypressKeyReaderPtr CypressKeyReader_;
    TSignatureValidatorPtr UnderlyingValidator_;
    ISignatureValidatorPtr SignatureValidator_;

    TCypressKeyWriterPtr CypressKeyWriter_;
    TSignatureGeneratorPtr UnderlyingGenerator_;
    TKeyRotatorPtr KeyRotator_;
    ISignatureGeneratorPtr SignatureGenerator_;
};

DEFINE_REFCOUNTED_TYPE(TSignatureComponents)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
