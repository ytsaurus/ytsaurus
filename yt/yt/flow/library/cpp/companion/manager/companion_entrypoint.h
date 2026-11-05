#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

//! Language-agnostic description of how to spawn a companion process.
struct TCompanionEntrypoint
    : public NYTree::TYsonStruct
{
    //! Absolute path to the executable to spawn (e.g. python companion binary or `java`).
    std::string Executable;

    //! Arguments passed to the executable.
    std::vector<std::string> Args;

    //! Additional environment variables passed to the spawned process.
    THashMap<std::string, std::string> Env;

    REGISTER_YSON_STRUCT(TCompanionEntrypoint);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCompanionEntrypoint);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
