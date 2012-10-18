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

    //! Maximum number to objects to destroy per a single GC mutation.
    int MaxObjectsPerGCSweep;

    //! Period between subsequent GC queue checks.
    TDuration GCSweepPeriod;

    //! Amount of time to wait before yielding meta state thread to another request.
    TDuration YieldTimeout;

    TObjectManagerConfig()
    {
        Register("cell_id", CellId)
            .Default(0);
        Register("max_objects_per_gc_sweep", MaxObjectsPerGCSweep)
            .Default(1000);
        Register("gc_sweep_period", GCSweepPeriod)
            .Default(TDuration::MilliSeconds(1000));
        Register("yield_timeout", YieldTimeout)
            .Default(TDuration::MilliSeconds(10));
    }
};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NObjectServer
} // namespace NYT
