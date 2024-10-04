#pragma once

#include "public.h"

#include "key_store.h"

namespace NYT::NSignatureService {

////////////////////////////////////////////////////////////////////////////////

class TSignatureValidator
{
public:
    explicit TSignatureValidator(IKeyStoreReader* store);

    TFuture<bool> Validate(TSignaturePtr signature);

private:
    IKeyStoreReader* const Store_;
    const NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignatureService
