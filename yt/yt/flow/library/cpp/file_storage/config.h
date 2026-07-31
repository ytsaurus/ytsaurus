#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow::NFileStorage {

////////////////////////////////////////////////////////////////////////////////

struct TFileStorageConfig
    : public NYTree::TYsonStruct
{
    std::string Path;
    i64 SoftSizeLimit{};
    i64 HardSizeLimit{};
    TDuration CleanupPeriod;

    REGISTER_YSON_STRUCT(TFileStorageConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFileStorageConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NFileStorage
