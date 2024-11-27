#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NDiskManager {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((DiskIdsMismatched)(3400))
);

// NB: Keep values in sync with diskman.proto.
DEFINE_ENUM(EDiskState,
    ((Unknown)     (0))
    ((OK)          (1))
    ((Failed)      (2))
    ((RecoverWait) (3))
);

DEFINE_ENUM_UNKNOWN_VALUE(EDiskState, Unknown);

// NB: Names matter; see |storage_class| in diskman.proto.
DEFINE_ENUM(EStorageClass,
    ((Unknown)     (0))
    ((Hdd)         (1))
    ((Ssd)         (2))
    ((Nvme)        (3))
    ((Virt)        (4))
);

DEFINE_ENUM_UNKNOWN_VALUE(EStorageClass, Unknown);

// 1. Remount all disk volumes to it's default state
// 2. Recreate disk layout, all data on disk will be lost
// 3. Replace phisical disk
// NB: Keep values in sync with diskman.proto.
DEFINE_ENUM(ERecoverPolicy,
    ((RecoverAuto)   (0))
    ((RecoverMount)  (1))
    ((RecoverLayout) (2))
    ((RecoverDisk)   (3))
);

DEFINE_ENUM_UNKNOWN_VALUE(ERecoverPolicy, RecoverAuto);

////////////////////////////////////////////////////////////////////////////////

struct TDiskInfo
{
    std::string DiskId;
    std::string DevicePath;
    std::string DeviceName;
    std::string DiskModel;
    THashSet<std::string> PartitionFsLabels;
    EDiskState State;
    EStorageClass StorageClass;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TDiskManagerProxyConfig)
DECLARE_REFCOUNTED_STRUCT(TDiskManagerProxyDynamicConfig)

DECLARE_REFCOUNTED_STRUCT(TDiskInfoProviderConfig)

DECLARE_REFCOUNTED_STRUCT(THotswapManagerConfig)
DECLARE_REFCOUNTED_STRUCT(THotswapManagerDynamicConfig)

DECLARE_REFCOUNTED_STRUCT(IDiskManagerProxy)
DECLARE_REFCOUNTED_STRUCT(IDiskInfoProvider)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiskManager
