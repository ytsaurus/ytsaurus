#include "cgroups_new.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/fs.h>

#include <util/stream/file.h>
#include <util/string/vector.h>

namespace NYT::NContainers::NCGroups {

////////////////////////////////////////////////////////////////////////////////

static inline const NLogging::TLogger Logger("CGroups");

////////////////////////////////////////////////////////////////////////////////

THashMap<TString, ui64> ReadAndParseStatFile(const TString& fileName)
{
    THashMap<TString, ui64> statistics;

    auto rawStatFile = TFileInput(fileName).ReadAll();
    for (auto line : SplitString(rawStatFile, "\n")) {
        auto fields = SplitString(line, " ");
        if (fields.size() != 2) {
            continue;
        }

        EmplaceOrCrash(statistics, fields[0], FromString<ui64>(fields[1]));
    }

    return statistics;
}

ui64 ReadAndParseValueFile(const TString& fileName)
{
    auto rawValueFile = TFileInput(fileName).ReadLine();
    return FromString<ui64>(rawValueFile);
}

////////////////////////////////////////////////////////////////////////////////

TMemoryStatistics GetMemoryStatisticsFromMemoryStat(const TString& path)
{
    auto statistics = ReadAndParseStatFile(Format("%/%", path, "memory.stat"));

    return TMemoryStatistics{
        .Rss = statistics["rss"],
        .MappedFile = statistics["mapped_file"],
        .MajorPageFaults = statistics["pgmajfault"],
    };
}

TMemoryStatistics GetMemoryStatisticsV1(const TString& cgroup)
{
    auto memoryStatPath = Format("/sys/fs/cgroup/memory/%v/memory.stat", cgroup);
    return GetMemoryStatisticsFromMemoryStat(memoryStatPath);
}

TMemoryStatistics GetMemoryStatisticsV2(const TString& cgroup)
{
    auto memoryStatPath = Format("/sys/fs/cgroup/%v/memory.stat", cgroup);
    auto statistics = GetMemoryStatisticsFromMemoryStat(memoryStatPath);

    // NB: In cgroups v2, rss is not in memory.stat, but in memory.current.
    // See https://stackoverflow.com/questions/74796436/rss-memory-equivalent-in-cgroup-v2.
    auto memoryCurrentPath = Format("/sys/fs/cgroup/%v/memory.current", cgroup);
    statistics.Rss = ReadAndParseValueFile(memoryCurrentPath);

    return statistics;
}

////////////////////////////////////////////////////////////////////////////////

TCpuStatistics GetCpuStatisticsV1(const TString& cgroup)
{
    auto cpuAcctStatPath = Format("/sys/fs/cgroup/cpuacct/%v/cpuacct.stat", cgroup);
    auto cpuAcctStatistics = ReadAndParseStatFile(cpuAcctStatPath);

    ui64 ticksPerSecond;
#if defined(__linux__)
    ticksPerSecond = sysconf(_SC_CLK_TCK);
#else
    ticksPerSecond = 1'000'000'000;
#endif

    auto fromJiffies = [&] (ui64 jiffies) {
        return TDuration::MicroSeconds(1'000'000 * jiffies / ticksPerSecond);
    };

    return TCpuStatistics{
        .UserTime = fromJiffies(cpuAcctStatistics["user"]),
        .SystemTime = fromJiffies(cpuAcctStatistics["system"]),
    };
}

TCpuStatistics GetCpuStatisticsV2(const TString& cgroup)
{
    auto cpuStatPath = Format("/sys/fs/cgroup/%v/cpu.stat", cgroup);
    auto cpuStatistics = ReadAndParseStatFile(cpuStatPath);

    return TCpuStatistics{
        .UserTime = TDuration::MicroSeconds(cpuStatistics["user_usec"]),
        .SystemTime = TDuration::MicroSeconds(cpuStatistics["system_usec"]),
    };
}

////////////////////////////////////////////////////////////////////////////////

THashMap<TString, ui64> ReadAndParseBlkIOStatFile(const TString& fileName)
{
    THashMap<TString, ui64> statistics;

    auto rawStatFile = TFileInput(fileName).ReadAll();
    for (const auto& line : SplitString(rawStatFile, "\n")) {
        auto fields = SplitString(line, " ");
        if (fields.size() != 3) {
            continue;
        }

        statistics[fields[1]] += FromString<ui64>(fields[2]);
    }

    return statistics;
}

THashMap<TString, ui64> ReadAndParseIOStatFile(const TString& fileName)
{
    THashMap<TString, ui64> statistics;

    auto rawStatFile = TFileInput(fileName).ReadAll();
    for (auto line : SplitString(rawStatFile, "\n")) {
        auto fields = SplitString(line, " ");
        for (int index = 1; index < std::ssize(fields); ++index) {
            auto tokens = SplitString(fields[index], ":");
            if (tokens.size() != 2) {
                continue;
            }
            statistics[tokens[0]] += FromString<ui64>(tokens[1]);
        }
    }

    return statistics;
}

TBlockIOStatistics GetBlockIOStatisticsV1(const TString& cgroup)
{
    TBlockIOStatistics statistics;

    for (auto fileName : {"blkio.io_service_bytes_recursive", "blkio.throttle.io_service_bytes_recursive"}) {
        auto filePath = Format("/sys/fs/cgroup/blkio/%v/%v", cgroup, fileName);
        if (!NFS::Exists(filePath)) {
            continue;
        }

        auto blkioStatistics = ReadAndParseBlkIOStatFile(filePath);
        statistics.IOReadByte += blkioStatistics["Read"];
        statistics.IOWriteByte += blkioStatistics["Write"];
        break;
    }

    for (auto fileName : {"blkio.io_serviced_recursive", "blkio.throttle.io_serviced_recursive"}) {
        auto filePath = Format("/sys/fs/cgroup/blkio/%v/%v", cgroup, fileName);
        if (!NFS::Exists(filePath)) {
            continue;
        }

        auto blkioStatistics = ReadAndParseBlkIOStatFile(filePath);
        statistics.IOReadOps += blkioStatistics["Read"];
        statistics.IOWriteOps += blkioStatistics["Write"];
        break;
    }

    return statistics;
}

TBlockIOStatistics GetBlockIOStatisticsV2(const TString& cgroup)
{
    auto ioStatPath = Format("/sys/fs/cgroup/%v/io.stat", cgroup);
    auto ioStatistics = ReadAndParseIOStatFile(ioStatPath);

    return TBlockIOStatistics{
        .IOReadByte = ioStatistics["rbytes"],
        .IOWriteByte = ioStatistics["wbytes"],
        .IOReadOps = ioStatistics["rios"],
        .IOWriteOps = ioStatistics["wios"],
    };
}

////////////////////////////////////////////////////////////////////////////////

TSelfCGroupsStatisticsFetcher::TSelfCGroupsStatisticsFetcher()
{
    DetectSelfCGroup();

    YT_LOG_INFO("CGroups statistics fetcher initialized (CGroup: %v, IsV2: %v)",
        CGroup_,
        IsV2_);
}

TMemoryStatistics TSelfCGroupsStatisticsFetcher::GetMemoryStatistics() const
{
    auto statistics = IsV2_ ? GetMemoryStatisticsV2(CGroup_) : GetMemoryStatisticsV1(CGroup_);

    {
        auto guard = Guard(SpinLock_);
        PeakRss_ = std::max(PeakRss_, statistics.Rss);
        statistics.PeakRss = PeakRss_;
    }

    return statistics;
}

TCpuStatistics TSelfCGroupsStatisticsFetcher::GetCpuStatistics() const
{
    return IsV2_ ? GetCpuStatisticsV2(CGroup_) : GetCpuStatisticsV1(CGroup_);
}

TBlockIOStatistics TSelfCGroupsStatisticsFetcher::GetBlockIOStatistics() const
{
    return IsV2_ ? GetBlockIOStatisticsV2(CGroup_) : GetBlockIOStatisticsV1(CGroup_);
}

void TSelfCGroupsStatisticsFetcher::DetectSelfCGroup()
{
    // NB: There are issues with cgroup namespaces in Kubernetes
    // (see https://github.com/kubernetes/enhancements/pull/1370),
    // so sometimes /proc/self/cgroup contains real cgroup path
    // but in /sys/fs/cgroup it is just root cgroup.
    // We will try our best to detect such a situation below.

    IsV2_ = true;
    CGroup_ = "/";

    auto rawSelfCGroups = TFileInput("/proc/self/cgroup").ReadAll();
    for (auto line : SplitString(rawSelfCGroups, "\n")) {
        auto tokens = SplitString(line, ":");
        if (tokens.size() != 3) {
            continue;
        }
        if (tokens[1] == "memory") {
            auto cgroup = tokens[2];
            if (NFS::Exists(Format("/sys/fs/cgroup/memory/%v/memory.stat", cgroup))) {
                CGroup_ = cgroup;
                IsV2_ = false;
                return;
            } else if (NFS::Exists("/sys/fs/cgroup/memory/memory.stat")) {
                CGroup_ = "/";
                IsV2_ = false;
                return;
            }
        } else if (tokens[1] == "") {
            auto cgroup = tokens[2];
            if (NFS::Exists(Format("/sys/fs/cgroup/%v/memory.stat", cgroup))) {
                CGroup_ = cgroup;
                IsV2_ = true;
                return;
            } else if (NFS::Exists("/sys/fs/cgroup/memory.stat")) {
                CGroup_ = "/";
                IsV2_ = true;
                return;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers::NCgroups
