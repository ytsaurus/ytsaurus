#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

#include <core/rpc/config.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

class TPeerConnectionConfig
    : public NRpc::TRetryingChannelConfig
    , public NRpc::TBalancingChannelConfig
{
public:
    //! Id of the cell.
    TCellGuid CellGuid;

    TPeerConnectionConfig()
    {
        RegisterParameter("cell_guid", CellGuid)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TPeerConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
