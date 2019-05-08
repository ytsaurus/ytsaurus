#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

#include <cmath>

namespace NYT::NCoreDump {

////////////////////////////////////////////////////////////////////////////////

class TCoreDumperConfig
    : public NYTree::TYsonSerializable
{
public:
    //! A path to store the core files.
    TString Path;

    //! A name under which the core file should be placed.
    //! Some of the porto variables like %CORE_PID, %CORE_TID etc are supported, refer to the implementation.
    TString Pattern;

    TCoreDumperConfig()
    {
        RegisterParameter("path", Path);
        RegisterParameter("pattern", Pattern)
            .Default("core.%CORE_DATETIME.%CORE_PID.%CORE_SIG.%CORE_THREAD_NAME-%CORE_REASON");
    }
};

DEFINE_REFCOUNTED_TYPE(TCoreDumperConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCoreDump
