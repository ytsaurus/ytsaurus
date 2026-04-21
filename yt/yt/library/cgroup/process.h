#pragma once

#include <yt/yt/core/misc/error.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

class TCpuAccounting
{
public:
    struct TStatistics
    {
        TErrorOr<TDuration> BurstUsageTime;
        TErrorOr<TDuration> TotalUsageTime;
        TErrorOr<TDuration> UserUsageTime;
        TErrorOr<TDuration> SystemUsageTime;
        TErrorOr<TDuration> WaitTime;
        TErrorOr<TDuration> ThrottledTime;
        TErrorOr<TDuration> CfsThrottledTime;

        TErrorOr<i64> ThreadCount;
        TErrorOr<i64> ContextSwitches;
        TErrorOr<i64> ContextSwitchesDelta;
        TErrorOr<i64> PeakThreadCount;

        TErrorOr<TDuration> LimitTime;
        TErrorOr<TDuration> GuaranteeTime;
    };
};

////////////////////////////////////////////////////////////////////////////////

class TBlockIO
{
public:
    struct TStatistics
    {
        struct TIOStatistics
        {
            TErrorOr<i64> IOReadByte{TError("Resource usage is missing IOReadBytes field")};
            TErrorOr<i64> IOWriteByte{TError("Resource usage is missing IOWriteByte field")};
            TErrorOr<i64> IOBytesLimit{TError("Resource usage is missing IOBytesLimit field")};

            TErrorOr<i64> IOReadOps{TError("Resource usage is missing IOReadOps field")};
            TErrorOr<i64> IOWriteOps{TError("Resource usage is missing IOWriteOps field")};
            TErrorOr<i64> IOOps{TError("Resource usage is missing IOOps field")};
            TErrorOr<i64> IOOpsLimit{TError("Resource usage is missing IOOpsLimit field")};

            TErrorOr<TDuration> IOTotalTime{TError("Resource usage is missing IOTotalTime field")};
            TErrorOr<TDuration> IOWaitTime{TError("Resource usage is missing IOWaitTime field")};
        };

        using TDeviceIOStatistics = THashMap<TString, TIOStatistics>;

        TIOStatistics TotalIOStatistics;
        TDeviceIOStatistics DeviceIOStatistics;
    };

    struct TStatisticsItem
    {
        TString DeviceId;
        TString Type;
        i64 Value = 0;
    };
};

////////////////////////////////////////////////////////////////////////////////

class TMemory
{
public:
    struct TStatistics
    {
        TErrorOr<i64> ResidentAnon;
        TErrorOr<i64> TmpfsUsage;
        TErrorOr<i64> MappedFile;
        TErrorOr<i64> MinorPageFaults;
        TErrorOr<i64> MajorPageFaults;

        TErrorOr<i64> FileCacheUsage;
        TErrorOr<i64> AnonUsage;
        TErrorOr<i64> AnonLimit;
        TErrorOr<i64> MemoryUsage;
        TErrorOr<i64> MemoryGuarantee;
        TErrorOr<i64> MemoryLimit;
        TErrorOr<i64> MaxMemoryUsage;

        TErrorOr<i64> OomKills;
        TErrorOr<i64> OomKillsTotal;
    };
};

////////////////////////////////////////////////////////////////////////////////

class TNetwork
{
public:
    struct TStatistics
    {
        TErrorOr<i64> TxBytes;
        TErrorOr<i64> TxPackets;
        TErrorOr<i64> TxDrops;
        TErrorOr<i64> TxLimit;

        TErrorOr<i64> RxBytes;
        TErrorOr<i64> RxPackets;
        TErrorOr<i64> RxDrops;
        TErrorOr<i64> RxLimit;
    };
};

////////////////////////////////////////////////////////////////////////////////

THashMap<TString, TString> ParseProcessCGroups(const TString& str);
THashMap<TString, TString> GetProcessCGroups(pid_t pid);
THashMap<TString, TString> GetSelfProcessCGroups();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
