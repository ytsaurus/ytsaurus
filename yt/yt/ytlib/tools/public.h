#pragma once

#include <yt/core/misc/public.h>

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
