#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/ytree/yson_struct.h>
#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/misc/property.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <vector>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

void RemoveAllSubcgroups(const TString& path);

////////////////////////////////////////////////////////////////////////////////

struct TKillProcessGroupTool
{
    void operator()(const TString& processGroupPath) const;
};

////////////////////////////////////////////////////////////////////////////////

class TNonOwningCGroup
    : private TNonCopyable
{
public:
    DEFINE_BYREF_RO_PROPERTY(TString, FullPath);

public:
    TNonOwningCGroup() = default;
    explicit TNonOwningCGroup(const TString& fullPath);
    TNonOwningCGroup(const TString& type, const TString& name);
    TNonOwningCGroup(TNonOwningCGroup&& other);

    void AddTask(int pid) const;
    void AddCurrentTask() const;

    bool IsRoot() const;
    bool IsNull() const;
    bool Exists() const;

    std::vector<int> GetProcesses() const;
    std::vector<int> GetTasks() const;
    const TString& GetFullPath() const;

    std::vector<TNonOwningCGroup> GetChildren() const;

    void EnsureExistence() const;

    void Lock() const;
    void Unlock() const;

    void Kill() const;

    void RemoveAllSubcgroups() const;
    void RemoveRecursive() const;

protected:
    TString Get(const TString& name) const;
    void Set(const TString& name, const TString& value) const;
    void Append(const TString& name, const TString& value) const;

    void DoLock() const;
    void DoUnlock() const;

    bool TryUnlock() const;

    void DoKill() const;

    void DoRemove() const;

    void Traverse(
        const TCallback<void(const TNonOwningCGroup&)>& preorderAction,
        const TCallback<void(const TNonOwningCGroup&)>& postorderAction) const;

    TString GetPath(const TString& filename) const;
};

////////////////////////////////////////////////////////////////////////////////

class TCGroup
    : public TNonOwningCGroup
{
protected:
    TCGroup(const TString& type, const TString& name);
    TCGroup(TNonOwningCGroup&& other);
    TCGroup(TCGroup&& other);

public:
    ~TCGroup();

    void Create();
    void Destroy();

    bool IsCreated() const;

private:
    bool Created_ = false;
};

////////////////////////////////////////////////////////////////////////////////

class TCpuAccounting
    : public TCGroup
{
public:
    static const TString Name;

    struct TStatistics
    {
        TErrorOr<TDuration> TotalUsageTime;
        TErrorOr<TDuration> UserUsageTime;
        TErrorOr<TDuration> SystemUsageTime;
        TErrorOr<TDuration> WaitTime;
        TErrorOr<TDuration> ThrottledTime;

        TErrorOr<i64> ThreadCount;
        TErrorOr<i64> ContextSwitches;
        TErrorOr<i64> ContextSwitchesDelta;
        TErrorOr<i64> PeakThreadCount;

        TErrorOr<TDuration> LimitTime;
        TErrorOr<TDuration> GuaranteeTime;
    };

    explicit TCpuAccounting(const TString& name);

    TStatistics GetStatisticsRecursive() const;
    TStatistics GetStatistics() const;

private:
    explicit TCpuAccounting(TNonOwningCGroup&& nonOwningCGroup);
};

void Serialize(const TCpuAccounting::TStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TCpu
    : public TCGroup
{
public:
    static const TString Name;

    explicit TCpu(const TString& name);

    void SetShare(double share);
};

////////////////////////////////////////////////////////////////////////////////

class TBlockIO
    : public TCGroup
{
public:
    static const TString Name;

    struct TStatistics
    {
        TErrorOr<i64> IOReadByte;
        TErrorOr<i64> IOWriteByte;
        TErrorOr<i64> IOBytesLimit;

        TErrorOr<i64> IOReadOps;
        TErrorOr<i64> IOWriteOps;
        TErrorOr<i64> IOOps;
        TErrorOr<i64> IOOpsLimit;

        TErrorOr<TDuration> IOTotalTime;
        TErrorOr<TDuration> IOWaitTime;
    };

    struct TStatisticsItem
    {
        TString DeviceId;
        TString Type;
        i64 Value = 0;
    };

    explicit TBlockIO(const TString& name);

    TStatistics GetStatistics() const;
    void ThrottleOperations(i64 iops) const;

private:
    //! Guards device ids.
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    //! Set of all seen device ids.
    mutable THashSet<TString> DeviceIds_;

    std::vector<TBlockIO::TStatisticsItem> GetDetailedStatistics(const char* filename) const;

    std::vector<TStatisticsItem> GetIOServiceBytes() const;
    std::vector<TStatisticsItem> GetIOServiced() const;
};

void Serialize(const TBlockIO::TStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TMemory
    : public TCGroup
{
public:
    static const TString Name;

    struct TStatistics
    {
        TErrorOr<i64> Rss;
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

    explicit TMemory(const TString& name);

    TStatistics GetStatistics() const;
    i64 GetMaxMemoryUsage() const;

    void SetLimitInBytes(i64 bytes) const;

    void ForceEmpty() const;
};

void Serialize(const TMemory::TStatistics& statistics, NYson::IYsonConsumer* consumer);

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

void Serialize(const TNetwork::TStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TFreezer
    : public TCGroup
{
public:
    static const TString Name;

    explicit TFreezer(const TString& name);

    TString GetState() const;
    void Freeze() const;
    void Unfreeze() const;
};

////////////////////////////////////////////////////////////////////////////////

THashMap<TString, TString> ParseProcessCGroups(const TString& str);
THashMap<TString, TString> GetProcessCGroups(pid_t pid);
THashMap<TString, TString> GetSelfProcessCGroups();
bool IsValidCGroupType(const TString& type);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
