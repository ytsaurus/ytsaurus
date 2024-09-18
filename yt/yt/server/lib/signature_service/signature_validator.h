#pragma once

#include "public.h"

#include "key_store.h"

namespace NYT::NSignatureService {

////////////////////////////////////////////////////////////////////////////////

class TSignatureValidator
{
public:
    explicit TSignatureValidator(IKeyStoreReader* store);

    // TODO(pavook) async?
    [[nodiscard]] bool Validate(const TSignature& signature);

private:
    IKeyStoreReader* const Store_;
    const NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignatureService
