#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NTools {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TSignalerConfig)

DECLARE_REFCOUNTED_CLASS(TMountTmpfsConfig)

DECLARE_REFCOUNTED_CLASS(TUmountConfig)

DECLARE_REFCOUNTED_CLASS(TExtractTarConfig)

DECLARE_REFCOUNTED_CLASS(TSetThreadPriorityConfig)

DECLARE_REFCOUNTED_CLASS(TFSQuotaConfig)

DECLARE_REFCOUNTED_CLASS(TChownChmodConfig)

DECLARE_REFCOUNTED_CLASS(TCopyDirectoryContentConfig)

DECLARE_REFCOUNTED_CLASS(TSendSignalConfig)

DECLARE_REFCOUNTED_STRUCT(TSlotConfig)
DECLARE_REFCOUNTED_STRUCT(TSlotLocationBuilderConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
