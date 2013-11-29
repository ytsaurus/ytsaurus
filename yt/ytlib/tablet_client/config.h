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
            .GreaterThan(0);
    }
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT
