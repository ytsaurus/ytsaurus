#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EDiskState,
    ((Unknown)     (0))
    ((Ok)          (1))
    ((Failed)      (2))
    ((RecoverWait) (3))
);

struct TDiskInfo
{
    TString DiskId;
    TString DevicePath;
    TString DeviceName;
    TString DiskModel;
    EDiskState State;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TDiskManagerProxyConfig)
DECLARE_REFCOUNTED_STRUCT(TDiskManagerProxyDynamicConfig)

DECLARE_REFCOUNTED_CLASS(TDiskManagerProxy)
DECLARE_REFCOUNTED_CLASS(TDiskInfoProvider)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
