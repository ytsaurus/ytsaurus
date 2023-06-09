#include "plugin.h"

#include <ydb/library/yql/utils/log/log.h>

namespace NYT::NYqlPlugin {

////////////////////////////////////////////////////////////////////////////////

Y_WEAK std::unique_ptr<IYqlPlugin> CreateYqlPlugin(TYqlPluginOptions& /*options*/) noexcept
{
    YQL_LOG(FATAL) << "No YQL plugin implementation is available; link against either "
                   << "yt/yql/plugin/native or yt/yql/plugin/dynamic";
    exit(1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
