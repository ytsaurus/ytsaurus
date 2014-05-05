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
    TCGroup(const Stroka& type, const Stroka& parent, const Stroka& name);
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

    TCpuAccounting(const Stroka& parent, const Stroka& name);
    TStats GetStats();
};

////////////////////////////////////////////////////////////////////////////////

class TBlockIO
    : public TCGroup
{
public:
    struct TStats
    {
        int64_t Sectors;
        int64_t BytesRead;
        int64_t BytesWritten;
    };

    TBlockIO(const Stroka& parent, const Stroka& name);
    TStats GetStats();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCGroup
} // namespace NYT
