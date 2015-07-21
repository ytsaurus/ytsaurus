#pragma once

#include "public.h"

#include <core/yson/public.h>

#include <vector>

namespace NYT {
namespace NCGroup {

////////////////////////////////////////////////////////////////////////////////

void RemoveAllSubcgroups(const Stroka& path);

void RunKiller(const Stroka& processGroupPath);

////////////////////////////////////////////////////////////////////////////////

struct TKillProcessGroupTool
{
    void operator()(const Stroka& processGroupPath) const;
};

////////////////////////////////////////////////////////////////////////////////

class TNonOwningCGroup
    : private TNonCopyable
{
public:
    TNonOwningCGroup() = default;
    explicit TNonOwningCGroup(const Stroka& fullPath);
    TNonOwningCGroup(const Stroka& type, const Stroka& name);
    TNonOwningCGroup(TNonOwningCGroup&& other);

    void AddTask(int pid) const;
    void AddCurrentTask() const;

    bool IsRoot() const;
    bool IsNull() const;
    bool Exists() const;

    std::vector<int> GetTasks() const;
    const Stroka& GetFullPath() const;

    std::vector<TNonOwningCGroup> GetChildren() const;

    void EnsureExistance() const;

    void Lock() const;
    void Unlock() const;

    void Kill() const;

    void RemoveAllSubcgroups() const;
    void RemoveRecursive() const;

protected:
    Stroka Get(const Stroka& name) const;
    void Set(const Stroka& name, const Stroka& value) const;
    void Append(const Stroka& name, const Stroka& value) const;

    void DoLock() const;
    void DoUnlock() const;

    bool TryUnlock() const;

    void DoKill() const;

    void DoRemove() const;

    void Traverse(
        const TCallback<void(const TNonOwningCGroup&)> preorderAction,
        const TCallback<void(const TNonOwningCGroup&)> postorderAction) const;

    Stroka GetPath(const Stroka& filename) const;

    Stroka FullPath_;
};

////////////////////////////////////////////////////////////////////////////////

class TCGroup
    : public TNonOwningCGroup
{
protected:
    TCGroup(const Stroka& type, const Stroka& name);
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
    static const Stroka Name;

    struct TStatistics
    {
        TDuration UserTime;
        TDuration SystemTime;
    };

    explicit TCpuAccounting(const Stroka& name);

    TStatistics GetStatistics() const;
};

void Serialize(const TCpuAccounting::TStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TBlockIO
    : public TCGroup
{
public:
    static const Stroka Name;

    struct TStatistics
    {
        ui64 BytesRead = 0;
        ui64 BytesWritten = 0;
        ui64 IORead = 0;
        ui64 IOWrite = 0;
    };

    struct TStatisticsItem
    {
        Stroka DeviceId;
        Stroka Type;
        ui64 Value = 0;
    };

    explicit TBlockIO(const Stroka& name);

    TStatistics GetStatistics() const;

    std::vector<TStatisticsItem> GetIOServiceBytes() const;
    std::vector<TStatisticsItem> GetIOServiced() const;

    void ThrottleOperations(const Stroka& deviceId, i64 operations) const;

private:
    std::vector<TBlockIO::TStatisticsItem> GetDetailedStatistics(const char* filename) const;
};

void Serialize(const TBlockIO::TStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TMemory
    : public TCGroup
{
public:
    static const Stroka Name;

    struct TStatistics
    {
        ui64 Rss = 0;
        ui64 MappedFile = 0;
    };

    explicit TMemory(const Stroka& name);

    TStatistics GetStatistics() const;
    ui64 GetMaxMemoryUsage() const;

    void SetLimitInBytes(i64 bytes) const;

    void ForceEmpty() const;
};

void Serialize(const TMemory::TStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TFreezer
    : public TCGroup
{
public:
    static const Stroka Name;

    explicit TFreezer(const Stroka& name);

    Stroka GetState() const;
    void Freeze() const;
    void Unfreeze() const;
};

////////////////////////////////////////////////////////////////////////////////

std::map<Stroka, Stroka> ParseProcessCGroups(const Stroka& str);

template <typename T>
T GetCurrentCGroup()
{
    return T("");
}

bool IsValidCGroupType(const Stroka& type);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCGroup
} // namespace NYT
