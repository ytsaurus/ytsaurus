#pragma once

#include <yt/server/master/tablet_server/config.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

class TCellBalancerConfig
    : public NYTree::TYsonSerializable
{
public:
    bool EnableTabletCellSmoothing;

    TCellBalancerConfig()
    {
        RegisterParameter("enable_tablet_cell_smoothing", EnableTabletCellSmoothing)
            .Default(true);
    }
};

DEFINE_REFCOUNTED_TYPE(TCellBalancerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
