#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

#include <cmath>

namespace NYT {
namespace NCoreDump {

////////////////////////////////////////////////////////////////////////////////

class TCoreDumperConfig
    : public NYTree::TYsonSerializable
{
public:
    //! A path to store the core files.
    TString Path;

    //! A name that identifies current component (master/scheduler/node).
    TString ComponentName;

    TCoreDumperConfig()
    {
        RegisterParameter("path", Path);
        RegisterParameter("component_name", ComponentName);
    }
};

DEFINE_REFCOUNTED_TYPE(TCoreDumperConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCoreDump
} // namespace NYT
