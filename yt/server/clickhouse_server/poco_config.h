#pragma once

#include <yt/core/ytree/public.h>

#include <contrib/libs/poco/Util/include/Poco/Util/AbstractConfiguration.h>
#include <contrib/libs/poco/Util/include/Poco/Util/LayeredConfiguration.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

Poco::AutoPtr<Poco::Util::AbstractConfiguration> ConvertToPocoConfig(const NYTree::INodePtr& node);
Poco::AutoPtr<Poco::Util::LayeredConfiguration> ConvertToLayeredConfig(const NYTree::INodePtr& node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
