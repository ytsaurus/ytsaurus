#pragma once

#include <ytlib/cgroup/statistics.pb.h>

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

void ToProto(NProto::TCpuAccountingStats* protoStats, const TCpuAccounting::TStats& stats);

////////////////////////////////////////////////////////////////////////////////

class TBlockIO
    : public TCGroup
{
public:
    struct TStats
    {
        TStats();

        i64 TotalSectors;
        i64 BytesRead;
        i64 BytesWritten;
    };

    explicit TBlockIO(const Stroka& name);
    TStats GetStats();
};

void ToProto(NProto::TBlockIOStats* protoStats, const TBlockIO::TStats& stats);

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
