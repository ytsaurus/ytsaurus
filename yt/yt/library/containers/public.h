#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/misc/public.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((FailedToStartContainer)  (14000))
);

DEFINE_ENUM(EStatField,
    // CPU
    (CpuUsage)
    (CpuUserUsage)
    (CpuSystemUsage)
    (CpuWait)
    (CpuThrottled)
    (ContextSwitches)
    (ContextSwitchesDelta)
    (ThreadCount)
    (CpuLimit)
    (CpuGuarantee)

    // Memory
    (Rss)
    (MappedFile)
    (MajorPageFaults)
    (MinorPageFaults)
    (FileCacheUsage)
    (AnonMemoryUsage)
    (AnonMemoryLimit)
    (MemoryUsage)
    (MemoryGuarantee)
    (MemoryLimit)
    (MaxMemoryUsage)
    (OomKills)
    (OomKillsTotal)

    // IO
    (IOReadByte)
    (IOWriteByte)
    (IOBytesLimit)
    (IOReadOps)
    (IOWriteOps)
    (IOOps)
    (IOOpsLimit)
    (IOTotalTime)
    (IOWaitTime)

    // Network
    (NetTxBytes)
    (NetTxPackets)
    (NetTxDrops)
    (NetTxLimit)
    (NetRxBytes)
    (NetRxPackets)
    (NetRxDrops)
    (NetRxLimit)
);

DEFINE_ENUM(EEnablePorto,
    (None)
    (Isolate)
    (Full)
);

struct TBind
{
    TString SourcePath;
    TString TargetPath;
    bool IsReadOnly;
};

struct TRootFS
{
    TString RootPath;
    bool IsRootReadOnly;
    std::vector<TBind> Binds;
};

struct TDevice
{
    TString DeviceName;
    bool Enabled;
};

DECLARE_REFCOUNTED_STRUCT(IContainerManager)
DECLARE_REFCOUNTED_STRUCT(IInstanceLauncher)
DECLARE_REFCOUNTED_STRUCT(IInstance)
DECLARE_REFCOUNTED_STRUCT(IPortoExecutor)

DECLARE_REFCOUNTED_CLASS(TPortoHealthChecker)
DECLARE_REFCOUNTED_CLASS(TInstanceLimitsTracker)
DECLARE_REFCOUNTED_CLASS(TPortoProcess)
DECLARE_REFCOUNTED_CLASS(TPortoResourceTracker)
DECLARE_REFCOUNTED_CLASS(TPortoExecutorConfig)
DECLARE_REFCOUNTED_CLASS(TPortoExecutorDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TPodSpecConfig)

////////////////////////////////////////////////////////////////////////////////

bool IsValidCGroupType(const TString& type);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
