#pragma once

#include <util/datetime/base.h>

#include <util/generic/hash.h>

namespace NYT::NCGroups {

////////////////////////////////////////////////////////////////////////////////

struct TMemoryStatistics
{
    i64 ResidentAnon = 0;
    i64 TmpfsUsage = 0;
    i64 MappedFile = 0;
    i64 MajorPageFaults = 0;
    i64 Cache = 0;
    i64 RssHuge = 0;
    i64 Dirty = 0;
    i64 Writeback = 0;
};

struct TMemoryLimits
{
    std::optional<i64> MemoryLimit;
    std::optional<i64> AnonymousMemoryLimit;
};

struct TCpuStatistics
{
    TDuration UserTime;
    TDuration SystemTime;
};

struct TCpuThrottlingStatistics
{
    ui64 NrPeriods = 0;
    ui64 NrThrottled = 0;
    TDuration ThrottledTime;
    TDuration WaitTime;
};

struct TBlockIOStatistics
{
    i64 IOReadByte = 0;
    i64 IOWriteByte = 0;

    i64 IOReadOps = 0;
    i64 IOWriteOps = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ICGroupStatisticsFetcher
{
    virtual ~ICGroupStatisticsFetcher() = default;

    virtual bool IsV2() const = 0;

    virtual TMemoryStatistics GetMemoryStatistics() const = 0;
    virtual TMemoryLimits GetMemoryLimits() const = 0;
    virtual TCpuStatistics GetCpuStatistics() const = 0;
    virtual TCpuThrottlingStatistics GetCpuThrottlingStatistics() const = 0;
    virtual TBlockIOStatistics GetBlockIOStatistics() const = 0;
    virtual i64 GetOomKillCount() const = 0;
};

// NB(pavook): cgroup paths are detected once at construction and cached.
// If the process gets migrated to a different cgroup at runtime, the cached
// paths will be stale. This process cgroup migration is not a typical behavior
// from YT's standpoint, and it can't be feasibly supported: there will either be
// a window where the cgroup paths are stale, or the paths would be re-loaded
// on each statistic fetch, which is too costly.
//
// Also note that this class properly supports either V1-only or V2-only environments.
// Technically, there can be a "mixed" environment, where some (V1) controllers are
// mounted under /sys/fs/cgroup, and other (V2) controllers are mounted under /sys/fs/cgroup/unified.
// However, systemd explicitly [does not support this setup][1], so neither do we.
//
// [1]: https://github.com/systemd/systemd/issues/10107
struct TSelfCGroupsStatisticsFetcher
{
    static const ICGroupStatisticsFetcher* Get();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCGroups
