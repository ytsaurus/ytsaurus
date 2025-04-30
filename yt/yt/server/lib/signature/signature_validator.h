#pragma once

#include "public.h"

#include <yt/yt/client/signature/validator.h>

#include <yt/yt/core/actions/public.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

class TSignatureValidator
    : public ISignatureValidator
{
public:
    TSignatureValidator(TSignatureValidatorConfigPtr config, IKeyStoreReaderPtr keyReader);

    TFuture<bool> Validate(const TSignaturePtr& signature) const final;

private:
    const TSignatureValidatorConfigPtr Config_;
    const IKeyStoreReaderPtr KeyReader_;
};

DEFINE_REFCOUNTED_TYPE(TSignatureValidator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
