#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

class TSignatureComponents final
{
public:
    TSignatureComponents(
        const TSignatureComponentsConfigPtr& config,
        NApi::NNative::IClientPtr client,
        IInvokerPtr rotateInvoker);

    TFuture<void> StartRotation();

    TFuture<void> StopRotation();

    TFuture<void> RotateOutOfBand();

    const ISignatureValidatorPtr& GetSignatureValidator();
    const ISignatureGeneratorPtr& GetSignatureGenerator();

private:
    const NApi::NNative::IClientPtr Client_;
    const IInvokerPtr RotateInvoker_;

    TFuture<void> InitializeCryptographyFuture_;

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
