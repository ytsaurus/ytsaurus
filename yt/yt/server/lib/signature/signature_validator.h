#pragma once

#include "public.h"

#include "key_store.h"

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

class TSignatureValidator final
{
public:
    explicit TSignatureValidator(const IKeyStoreReaderPtr& store);

    TFuture<bool> Validate(TSignaturePtr signature);

private:
    const IKeyStoreReaderPtr Store_;
};

DEFINE_REFCOUNTED_TYPE(TSignatureValidator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
