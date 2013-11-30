#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NTabletClient {

///////////////////////////////////////////////////////////////////////////////

class TTableMountConfig
    : public TYsonSerializable
{
public:
    int MaxVersions;

    TTableMountConfig()
    {
        RegisterParameter("max_versions", MaxVersions)
            .Default(16)
            .GreaterThan(0)
            .LessThanOrEqual(65535);
    }
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT
