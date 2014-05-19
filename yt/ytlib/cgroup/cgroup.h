#pragma once

#include <util/generic/stroka.h>

#include <vector>
#include <chrono>

namespace NYT {
namespace NCGroup {

////////////////////////////////////////////////////////////////////////////////

class TCGroup
    : private TNonCopyable
{
protected:
    TCGroup(const Stroka& type, const Stroka& name);

public:
    ~TCGroup();

    void AddCurrentProcess();

    void Create();
    void Destroy();

    std::vector<int> GetTasks() const;
    const Stroka& GetFullPath() const;
    bool IsCreated() const;

private:
    Stroka FullPath_;
    bool Created_;
};

////////////////////////////////////////////////////////////////////////////////

class TCpuAccounting
    : public TCGroup
{
public:
    struct TStats
    {
        TStats();

        TDuration User;
        TDuration System;
    };

    explicit TCpuAccounting(const Stroka& name);
    TStats GetStats();
};

////////////////////////////////////////////////////////////////////////////////

class TBlockIO
    : public TCGroup
{
public:
    struct TStats
    {
        TStats();

        int64_t TotalSectors;
        int64_t BytesRead;
        int64_t BytesWritten;
    };

    explicit TBlockIO(const Stroka& name);
    TStats GetStats();
};

////////////////////////////////////////////////////////////////////////////////

class TMemory
    : public TCGroup
{
public:
    struct TStats
    {
    };

    explicit TMemory(const Stroka& name);
    TStats GetStats();
};

////////////////////////////////////////////////////////////////////////////////

std::map<Stroka, Stroka> ParseCurrentProcessCGroups(TStringBuf str);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCGroup
} // namespace NYT
