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
    explicit TSignatureValidator(TSignatureValidatorConfigPtr config, IKeyStoreReaderPtr keyReader);

    TFuture<bool> Validate(const TSignaturePtr& signature) override;

private:
    const TSignatureValidatorConfigPtr Config_;
    const IKeyStoreReaderPtr KeyReader_;
};

DEFINE_REFCOUNTED_TYPE(TSignatureValidator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
