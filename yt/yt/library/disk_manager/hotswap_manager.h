#pragma once

#include "public.h"

#include <yt/yt/core/ytree/public.h>

#include <util/generic/noncopyable.h>

namespace NYT::NDiskManager {

////////////////////////////////////////////////////////////////////////////////

class THotswapManager
    : private TNonCopyable
{
public:
    static void Configure(const THotswapManagerConfigPtr& config);
    static void Reconfigure(const THotswapManagerDynamicConfigPtr& dynamicConfig);

    static IDiskInfoProviderPtr GetDiskInfoProvider();
    static void PopulateAlerts(std::vector<TError>* alerts);
    static NYTree::IYPathServicePtr GetOrchidService();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiskManager

