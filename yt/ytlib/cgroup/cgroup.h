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

    std::vector<int> GetTasks();
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
        std::chrono::nanoseconds User;
        std::chrono::nanoseconds System;
    };

    TCpuAccounting(const Stroka& name);
    TStats GetStats();
};

////////////////////////////////////////////////////////////////////////////////

class TBlockIO
    : public TCGroup
{
public:
    struct TStats
    {
        int64_t TotalSectors;
        int64_t BytesRead;
        int64_t BytesWritten;
    };

    TBlockIO(const Stroka& name);
    TStats GetStats();
};

////////////////////////////////////////////////////////////////////////////////

std::map<Stroka, Stroka> ParseCurrentProcessCGrops(const char* str, size_t size);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCGroup
} // namespace NYT
