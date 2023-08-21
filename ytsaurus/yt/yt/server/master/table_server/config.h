#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TTableManagerConfig
    : public NYTree::TYsonStruct
{
public:
    // COMPAT(h0pless): Remove after RecomputeMasterTableSchemaRefCounters.
    bool AlertOnMasterTableSchemaRefCounterMismatch;

    REGISTER_YSON_STRUCT(TTableManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTableManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
