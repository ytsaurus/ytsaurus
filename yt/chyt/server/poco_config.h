#pragma once

#include <yt/yt/core/ytree/public.h>

#include <DBPoco/Util/AbstractConfiguration.h>
#include <DBPoco/Util/LayeredConfiguration.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DBPoco::AutoPtr<DBPoco::Util::AbstractConfiguration> ConvertToPocoConfig(const NYTree::INodePtr& node);
DBPoco::AutoPtr<DBPoco::Util::LayeredConfiguration> ConvertToLayeredConfig(const NYTree::INodePtr& node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
