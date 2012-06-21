#pragma once

#include "public.h"

#include <ytlib/misc/configurable.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

struct TObjectManagerConfig
    : public TConfigurable
{
    //! A number identifying the cell in the whole world.
    ui16 CellId;

    TObjectManagerConfig()
    {
        Register("cell_id", CellId)
            .Default(0);
    }
};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NObjectServer
} // namespace NYT
