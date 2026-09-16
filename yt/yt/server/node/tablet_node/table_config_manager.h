#include "public.h"

// TODO(ifsmirnov): extract interface part from dynamic config manager to avoid
// including certain config headers in .h.
#include <yt/yt/server/lib/tablet_node/table_settings.h>

#include <yt/yt/server/lib/tablet_node/public.h>

#include <yt/yt/library/dynamic_config/dynamic_config_manager.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TTableDynamicConfigManager
    : public NDynamicConfig::TDynamicConfigManagerBase<TClusterTableConfigPatchSet>
{
public:
    explicit TTableDynamicConfigManager(IBootstrap* bootstrap);
};

DEFINE_REFCOUNTED_TYPE(TTableDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
