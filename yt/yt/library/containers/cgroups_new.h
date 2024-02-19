#include "public.h"

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NContainers::NCGroups {

////////////////////////////////////////////////////////////////////////////////

struct TMemoryStatistics
{
    i64 ResidentAnon = 0;
    i64 PeakResidentAnon = 0;
    i64 MappedFile = 0;
    i64 MajorPageFaults = 0;
};

struct TCpuStatistics
{
    TDuration UserTime;
    TDuration SystemTime;
};

struct TBlockIOStatistics
{
    i64 IOReadByte = 0;
    i64 IOWriteByte = 0;

    i64 IOReadOps = 0;
    i64 IOWriteOps = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TSelfCGroupsStatisticsFetcher
{
public:
    TSelfCGroupsStatisticsFetcher();

    TMemoryStatistics GetMemoryStatistics() const;
    TCpuStatistics GetCpuStatistics() const;
    TBlockIOStatistics GetBlockIOStatistics() const;

private:
    TString CGroup_;
    bool IsV2_;

    mutable i64 PeakResidentAnon_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);

    void DetectSelfCGroup();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers::NCgroups
