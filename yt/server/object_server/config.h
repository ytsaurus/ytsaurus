#pragma once

#include "public.h"

#include <ytlib/ytree/yson_serializable.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

struct TObjectManagerConfig
    : public TYsonSerializable
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
