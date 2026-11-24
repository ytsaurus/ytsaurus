#pragma once

#include "provider_base.h"

namespace NYdb::inline V3::NOidc::NPrivate {

class TDeviceProvider final: public TRefreshingProviderBase {
public:
    TDeviceProvider(const TOidcConfig& config, std::weak_ptr<ICoreFacility> facility);
    ~TDeviceProvider() override;

private:
    TTokenCache AcquireToken() override;
};

} // namespace NYdb::inline V3::NOidc::NPrivate
