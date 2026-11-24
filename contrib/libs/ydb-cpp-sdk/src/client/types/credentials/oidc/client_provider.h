#pragma once

#include "provider_base.h"

namespace NYdb::inline V3::NOidc::NPrivate {

class TClientProvider final: public TRefreshingProviderBase {
public:
    TClientProvider(const TOidcConfig& config, std::weak_ptr<ICoreFacility> facility);
    ~TClientProvider() override;

private:
    TTokenCache AcquireToken() override;
};

} // namespace NYdb::inline V3::NOidc::NPrivate
