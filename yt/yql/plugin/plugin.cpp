#include "plugin.h"

#include <iostream>

namespace NYT::NYqlPlugin {

NYTree::IMapNodePtr IYqlPlugin::GetOrchidNode() const
{
    return NYTree::GetEphemeralNodeFactory()->CreateMap();
}

////////////////////////////////////////////////////////////////////////////////

Y_WEAK std::unique_ptr<IYqlPlugin> CreateYqlPlugin(TYqlPluginOptions /*options*/) noexcept
{
    std::cerr << "No YQL plugin implementation is available; link against either "
              << "yt/yql/plugin/native or yt/yql/plugin/dynamic" << std::endl;
    exit(1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
