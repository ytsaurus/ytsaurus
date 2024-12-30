#pragma once

#include "public.h"

#include <yt/yt/core/ytree/public.h>

namespace NYT::NDiskManager {

////////////////////////////////////////////////////////////////////////////////

struct IHotswapManager
    : public TRefCounted
{
    virtual void Reconfigure(const THotswapManagerDynamicConfigPtr& dynamicConfig) = 0;
    virtual IDiskInfoProviderPtr GetDiskInfoProvider() = 0;
    virtual void PopulateAlerts(std::vector<TError>* alerts) = 0;
    virtual NYTree::IYPathServicePtr GetOrchidService() = 0;
};

DEFINE_REFCOUNTED_TYPE(IHotswapManager)
YT_DEFINE_TYPEID(IHotswapManager)

////////////////////////////////////////////////////////////////////////////////

IHotswapManagerPtr CreateHotswapManager(THotswapManagerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiskManager

