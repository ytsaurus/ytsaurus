#pragma once

#include "public.h"

#include "key_store.h"

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

class TSignatureValidator
{
public:
    explicit TSignatureValidator(IKeyStoreReader* store);

    TFuture<bool> Validate(TSignaturePtr signature);

private:
    IKeyStoreReader* const Store_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
