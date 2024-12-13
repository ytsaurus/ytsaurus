#pragma once

#include <yt/yt/library/containers/config.h>

#include <yt/yt/library/disk_manager/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TServerProgramConfig
    : public virtual NYTree::TYsonStruct
{
    bool EnablePortoResourceTracker;
    NContainers::TPodSpecConfigPtr PodSpec;

    NDiskManager::THotswapManagerConfigPtr HotswapManager;

    REGISTER_YSON_STRUCT(TServerProgramConfig)

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServerProgramConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
