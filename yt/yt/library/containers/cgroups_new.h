#include "public.h"

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NContainers::NCGroups {

////////////////////////////////////////////////////////////////////////////////

struct TMemoryStatistics
{
    ui64 ResidentAnon = 0;
    ui64 TmpfsUsage = 0;
    ui64 MappedFile = 0;
    ui64 MajorPageFaults = 0;
};

struct TCpuStatistics
{
    TDuration UserTime;
    TDuration SystemTime;
};

struct TBlockIOStatistics
{
    ui64 IOReadByte = 0;
    ui64 IOWriteByte = 0;

    ui64 IOReadOps = 0;
    ui64 IOWriteOps = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TSelfCGroupsStatisticsFetcher
{
public:
    TSelfCGroupsStatisticsFetcher();

    TMemoryStatistics GetMemoryStatistics() const;
    TCpuStatistics GetCpuStatistics() const;
    TBlockIOStatistics GetBlockIOStatistics() const;
    i64 GetOOMKillCount() const;

private:
    TString CGroup_;
    bool IsV2_;

    void DetectSelfCGroup();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers::NCgroups
