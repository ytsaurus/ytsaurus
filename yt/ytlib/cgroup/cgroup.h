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
    struct TStatistics
    {
        TStatistics();

        TDuration UserTime;
        TDuration SystemTime;
    };

    explicit TCpuAccounting(const Stroka& name);
    TStatistics GetStatistics();
};

void ToProto(NProto::TCpuAccountingStatistics* protoStats, const TCpuAccounting::TStatistics& stats);

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

    explicit TBlockIO(const Stroka& name);
    TStatistics GetStatistics();
};

void ToProto(NProto::TBlockIOStatistics* protoStats, const TBlockIO::TStatistics& stats);

////////////////////////////////////////////////////////////////////////////////

class TMemory
    : public TCGroup
{
public:
    struct TStatistics
    {
        TStatistics();

        i64 TotalUsageInBytes;
    };

    explicit TMemory(const Stroka& name);
    TStatistics GetStatistics();
};

////////////////////////////////////////////////////////////////////////////////

std::map<Stroka, Stroka> ParseCurrentProcessCGroups(TStringBuf str);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCGroup
} // namespace NYT
