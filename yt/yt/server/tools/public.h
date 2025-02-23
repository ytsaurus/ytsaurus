#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NTools {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf ToolsProgramName = "ytserver-tools";

DECLARE_REFCOUNTED_STRUCT(TSignalerConfig)

DECLARE_REFCOUNTED_STRUCT(TMountTmpfsConfig)

DECLARE_REFCOUNTED_STRUCT(TSpawnShellConfig)

DECLARE_REFCOUNTED_STRUCT(TUmountConfig)

DECLARE_REFCOUNTED_STRUCT(TExtractTarConfig)

DECLARE_REFCOUNTED_STRUCT(TSetThreadPriorityConfig)

DECLARE_REFCOUNTED_STRUCT(TFSQuotaConfig)

DECLARE_REFCOUNTED_STRUCT(TChownChmodConfig)

DECLARE_REFCOUNTED_STRUCT(TGetDirectorySizesAsRootConfig)

DECLARE_REFCOUNTED_STRUCT(TCopyDirectoryContentConfig)

DECLARE_REFCOUNTED_STRUCT(TSendSignalConfig)

DECLARE_REFCOUNTED_STRUCT(TDirectoryConfig)
DECLARE_REFCOUNTED_STRUCT(TRootDirectoryConfig)
DECLARE_REFCOUNTED_STRUCT(TDirectoryBuilderConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
