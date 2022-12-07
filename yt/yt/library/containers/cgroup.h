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

    void EnsureExistance() const;

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

        TErrorOr<ui64> ThreadCount = 0;
        TErrorOr<ui64> ContextSwitches = 0;
        TErrorOr<ui64> PeakThreadCount = 0;

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
        TErrorOr<ui64> IOReadByte = 0;
        TErrorOr<ui64> IOWriteByte = 0;
        TErrorOr<ui64> IOBytesLimit = 0;

        TErrorOr<ui64> IOReadOps = 0;
        TErrorOr<ui64> IOWriteOps = 0;
        TErrorOr<ui64> IOOps = 0;
        TErrorOr<ui64> IOOpsLimit = 0;

        TErrorOr<TDuration> IOTotalTime;
        TErrorOr<TDuration> IOWaitTime;
    };

    struct TStatisticsItem
    {
        TString DeviceId;
        TString Type;
        ui64 Value = 0;
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
        TErrorOr<ui64> Rss = 0;
        TErrorOr<ui64> MappedFile = 0;
        TErrorOr<ui64> MinorPageFaults = 0;
        TErrorOr<ui64> MajorPageFaults = 0;

        TErrorOr<ui64> FileCacheUsage = 0;
        TErrorOr<ui64> AnonUsage = 0;
        TErrorOr<ui64> AnonLimit = 0;
        TErrorOr<ui64> MemoryUsage = 0;
        TErrorOr<ui64> MemoryGuarantee = 0;
        TErrorOr<ui64> MemoryLimit = 0;
        TErrorOr<ui64> MaxMemoryUsage = 0;
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
        TErrorOr<ui64> TxBytes = 0;
        TErrorOr<ui64> TxPackets = 0;
        TErrorOr<ui64> TxDrops = 0;
        TErrorOr<ui64> TxLimit = 0;

        TErrorOr<ui64> RxBytes = 0;
        TErrorOr<ui64> RxPackets = 0;
        TErrorOr<ui64> RxDrops = 0;
        TErrorOr<ui64> RxLimit = 0;
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

std::map<TString, TString> ParseProcessCGroups(const TString& str);
std::map<TString, TString> GetProcessCGroups(pid_t pid);
bool IsValidCGroupType(const TString& type);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
