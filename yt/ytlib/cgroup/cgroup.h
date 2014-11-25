#pragma once

#include "public.h"

#include <core/yson/public.h>

#include <ytlib/cgroup/statistics.pb.h>

#include <util/generic/stroka.h>

#include <vector>
#include <chrono>

namespace NYT {
namespace NCGroup {

////////////////////////////////////////////////////////////////////////////////

class TEvent
    : private TNonCopyable
{
public:
    TEvent();
    ~TEvent();

    TEvent(TEvent&& other);

    bool Fired();

    void Clear();
    void Destroy();

    TEvent& operator=(TEvent&& other);

    i64 GetLastValue() const;

protected:
    TEvent(int eventFd, int fd = -1);

private:
    void Swap(TEvent& other);

    int EventFd_;
    int Fd_;
    bool Fired_;
    i64 LastValue_;

    friend TMemory;
};

////////////////////////////////////////////////////////////////////////////////

std::vector<Stroka> GetSupportedCGroups();

void RemoveAllSubcgroups(const Stroka& path);

void RunKiller(const Stroka& processGroupPath);

void KillProcessGroup(const Stroka& processGroupPath);

////////////////////////////////////////////////////////////////////////////////

class TNonOwningCGroup
    : private TNonCopyable
{
public:
    TNonOwningCGroup();
    explicit TNonOwningCGroup(const Stroka& fullPath);
    TNonOwningCGroup(const Stroka& type, const Stroka& name);
    TNonOwningCGroup(TNonOwningCGroup&& other);

    void AddTask(int pid);
    void AddCurrentTask();

    Stroka Get(const Stroka& name) const;
    void Set(const Stroka& name, const Stroka& value) const;
    void Append(const Stroka& name, const Stroka& value) const;

    bool IsNull() const;
    std::vector<int> GetTasks() const;
    const Stroka& GetFullPath() const;

    void EnsureExistance();

protected:
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
    bool Created_;
};

////////////////////////////////////////////////////////////////////////////////

class TCpuAccounting
    : public TCGroup
{
public:
    struct TStatistics
    {
        TStatistics();

        TDuration UserTime;
        TDuration SystemTime;
    };

    explicit TCpuAccounting(const Stroka& name);
    TStatistics GetStatistics();
};

void Serialize(const TCpuAccounting::TStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TBlockIO
    : public TCGroup
{
public:
    struct TStatistics
    {
        TStatistics();

        i64 TotalSectors;
        i64 BytesRead;
        i64 BytesWritten;
    };

    struct TStatisticsItem
    {
        Stroka DeviceId;
        Stroka Type;
        i64 Value;
    };

    explicit TBlockIO(const Stroka& name);

    TStatistics GetStatistics();

    std::vector<TStatisticsItem> GetIOServiceBytes();
    std::vector<TStatisticsItem> GetIOServiced();

    void ThrottleOperations(const Stroka& deviceId, i64 operations);

private:
    std::vector<TBlockIO::TStatisticsItem> GetDetailedStatistics(const char* filename);
};

void Serialize(const TBlockIO::TStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TMemory
    : public TCGroup
{
public:
    struct TStatistics
    {
        TStatistics();

        i64 Rss;
        i64 MappedFile;
    };

    explicit TMemory(const Stroka& name);
    TMemory(TMemory&& other);
    TStatistics GetStatistics();

    i64 GetUsageInBytes() const;
    i64 GetMaxUsageInBytes() const;

    void SetLimitInBytes(i64 bytes) const;

    bool IsHierarchyEnabled() const;
    void EnableHierarchy() const;

    bool IsOomEnabled() const;
    void DisableOom() const;
    TEvent GetOomEvent() const;

    void ForceEmpty() const;

    int GetFailCount() const;
};

////////////////////////////////////////////////////////////////////////////////

class TFreezer
    : public TCGroup
{
public:
    explicit TFreezer(const Stroka& name);

    Stroka GetState() const;
    void Freeze() const;
    void UnFreeze() const;
};

////////////////////////////////////////////////////////////////////////////////

std::map<Stroka, Stroka> ParseCurrentProcessCGroups(TStringBuf str);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCGroup
} // namespace NYT
