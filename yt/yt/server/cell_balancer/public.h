#pragma once

#include <yt/yt/core/actions/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <util/generic/hash.h>

namespace NYT::NCellBalancer{

////////////////////////////////////////////////////////////////////////////////

template <class TEntryInfo>
using TIndexedEntries = THashMap<std::string, TIntrusivePtr<TEntryInfo>>;

using TDataCenterToInstanceMap = THashMap<std::string, std::vector<std::string>>;

struct TAlert;
using TOnAlertCallback = TCallback<void(TAlert)>;

struct TSchedulerMutations;
struct TSchedulerInputState;

DECLARE_REFCOUNTED_STRUCT(TAllocationRequest)
DECLARE_REFCOUNTED_STRUCT(TDeallocationRequest)
DECLARE_REFCOUNTED_STRUCT(TBundleControllerInstanceAnnotations)
DECLARE_REFCOUNTED_STRUCT(TAccountResources)
DECLARE_REFCOUNTED_STRUCT(TBundleConfig)
DECLARE_REFCOUNTED_STRUCT(TBundleControllerState)
DECLARE_REFCOUNTED_STRUCT(TBundleDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TZoneInfo)
DECLARE_REFCOUNTED_STRUCT(TBundleInfo)
DECLARE_REFCOUNTED_STRUCT(TInstanceInfoBase)
DECLARE_REFCOUNTED_STRUCT(TTabletNodeInfo)
DECLARE_REFCOUNTED_STRUCT(TTabletCellInfo)
DECLARE_REFCOUNTED_STRUCT(TRpcProxyInfo)
DECLARE_REFCOUNTED_STRUCT(TSystemAccount)
DECLARE_REFCOUNTED_STRUCT(TSysConfig)
DECLARE_REFCOUNTED_STRUCT(TCypressAnnotations)
DECLARE_REFCOUNTED_STRUCT(TDataCenterInfo)
DECLARE_REFCOUNTED_STRUCT(TAllocationRequestState)
DECLARE_REFCOUNTED_STRUCT(TDeallocationRequestState)
DECLARE_REFCOUNTED_STRUCT(IAllocatorAdapter)
DECLARE_REFCOUNTED_STRUCT(ISpareInstanceAllocator)
DECLARE_REFCOUNTED_STRUCT(TResourceQuota)

using TBundlesDynamicConfig = THashMap<std::string, TBundleDynamicConfigPtr>;

struct TDataCenterDisruptedState;

static const std::string DefaultDataCenterName = "default";

constexpr int DefaultWriteThreadPoolSize = 5;

template <class TSpareInstances>
struct TSpareInstanceAllocator;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
