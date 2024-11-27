#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

class TSignatureValidator final
{
public:
    explicit TSignatureValidator(TSignatureValidatorConfigPtr config, IKeyStoreReaderPtr store);

    TFuture<bool> Validate(const TSignaturePtr& signature);

private:
    const TSignatureValidatorConfigPtr Config_;
    const IKeyStoreReaderPtr Store_;
};

DEFINE_REFCOUNTED_TYPE(TSignatureValidator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
