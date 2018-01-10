#pragma once

#include "public.h"

#include <yt/core/actions/public.h>

#include <yt/core/yson/public.h>

#include <vector>

namespace NYT {
namespace NCGroup {

////////////////////////////////////////////////////////////////////////////////

void RemoveAllSubcgroups(const TString& path);

void RunKiller(const TString& processGroupPath);

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
    TNonOwningCGroup() = default;
    explicit TNonOwningCGroup(const TString& fullPath);
    TNonOwningCGroup(const TString& type, const TString& name);
    TNonOwningCGroup(TNonOwningCGroup&& other);

    void AddTask(int pid) const;
    void AddCurrentTask() const;

    bool IsRoot() const;
    bool IsNull() const;
    bool Exists() const;

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

    TString FullPath_;
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
        TDuration UserTime;
        TDuration SystemTime;
    };

    explicit TCpuAccounting(const TString& name);

    TStatistics GetStatisticsRecursive() const;
    TStatistics GetStatistics() const;

private:
    TCpuAccounting(TNonOwningCGroup&& nonOwningCGroup);
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
        ui64 BytesRead = 0;
        ui64 BytesWritten = 0;
        ui64 IORead = 0;
        ui64 IOWrite = 0;
        ui64 IOTotal = 0;
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
    TSpinLock SpinLock_;
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
        ui64 Rss = 0;
        ui64 MappedFile = 0;
        ui64 MajorPageFaults = 0;
    };

    explicit TMemory(const TString& name);

    TStatistics GetStatistics() const;
    i64 GetMaxMemoryUsage() const;

    void SetLimitInBytes(i64 bytes) const;

    void ForceEmpty() const;
};

void Serialize(const TMemory::TStatistics& statistics, NYson::IYsonConsumer* consumer);

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

template <typename T>
T GetCurrentCGroup()
{
    return T("");
}

bool IsValidCGroupType(const TString& type);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCGroup
} // namespace NYT
