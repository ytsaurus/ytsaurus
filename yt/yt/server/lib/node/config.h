#pragma once

#include "public.h"

#include <yt/yt/server/lib/io/public.h>
#include <yt/yt/server/lib/misc/config.h>

namespace NYT::NNode {

////////////////////////////////////////////////////////////////////////////////


struct TChunkLocationConfigBase
    : public NServer::TDiskLocationConfig
{
    static constexpr bool EnableHazard = true;

    //! Maximum space chunks are allowed to occupy.
    //! (If not initialized then indicates to occupy all available space on drive).
    std::optional<i64> Quota;

    NServer::TDiskHealthCheckerConfigPtr DiskHealthChecker;

    //! IO engine type.
    NIO::EIOEngineType IOEngineType;

    //! IO engine config.
    NYTree::INodePtr IOConfig;

    bool ResetUuid;

    //! Limit on the maximum memory used in location writes with legacy protocol without probing.
    // COMPAT(vvshlyaga): Remove after rolling writer with probing on all nodes.
    i64 LegacyWriteMemoryLimit;

    //! Limit on the maximum memory used of location reads.
    i64 ReadMemoryLimit;

    //! Limit on the maximum memory used of location writes.
    i64 WriteMemoryLimit;

    //! Limit on the maximum memory used of location reads and writes.
    i64 TotalMemoryLimit;

    //! Limit on the maximum count of location write sessions.
    i64 SessionCountLimit;

    void ApplyDynamicInplace(const TChunkLocationDynamicConfigBase& dynamicConfig);

    REGISTER_YSON_STRUCT(TChunkLocationConfigBase);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkLocationConfigBase)

struct TChunkLocationDynamicConfigBase
    : public NServer::TDiskLocationDynamicConfig
{
    std::optional<NIO::EIOEngineType> IOEngineType;
    NYTree::INodePtr IOConfig;

    NServer::TDiskHealthCheckerDynamicConfigPtr DiskHealthChecker;

    //! Limit on the maximum memory used in location writes with legacy protocol without probing.
    // COMPAT(vvshlyaga): Remove after rolling writer with probing on all nodes.
    std::optional<i64> LegacyWriteMemoryLimit;

    //! Limit on the maximum memory used by location reads.
    std::optional<i64> ReadMemoryLimit;

    //! Limit on the maximum memory used by location writes.
    std::optional<i64> WriteMemoryLimit;

    //! Limit on the maximum memory used by location reads and writes.
    std::optional<i64> TotalMemoryLimit;

    //! Limit on the maximum count of location write sessions.
    std::optional<i64> SessionCountLimit;

    REGISTER_YSON_STRUCT(TChunkLocationDynamicConfigBase);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkLocationDynamicConfigBase)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNode
