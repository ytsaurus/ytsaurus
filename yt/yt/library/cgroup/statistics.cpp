#include "statistics.h"

#include <library/cpp/yt/logging/logger.h>

#include <library/cpp/yt/misc/global.h>

#include <util/stream/file.h>

#include <util/string/vector.h>

#include <util/system/fs.h>

#include <linux/magic.h>

#include <sys/vfs.h>

namespace NYT::NCGroups {

////////////////////////////////////////////////////////////////////////////////

static YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "CGroups");

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

TString GetCGroupFilePath(const std::string& cgroupPath, const std::string& fileName)
{
    // NB: consecutive slashes in the resulting path are intentionally ignored
    // for clarity and ease of correctness across different environments.
    // Linux treats consecutive slashes as a single separator.
    return Format("/sys/fs/cgroup/%v/%v", cgroupPath, fileName);
}

THashMap<std::string, i64> ReadAndParseStatFile(const TString& fileName)
{
    THashMap<std::string, i64> statistics;

    TString rawStatFile;
    try {
        rawStatFile = TFileInput(fileName).ReadAll();
    } catch (const TFileError& error) {
        return {};
    }

    for (const auto& line : SplitString(rawStatFile, "\n")) {
        auto fields = SplitString(line, " ");
        if (fields.size() != 2) {
            continue;
        }

        auto [_, emplaced] = statistics.emplace(fields[0], FromString<i64>(fields[1]));
        YT_VERIFY(emplaced, Format("EmplaceOrCrash failed, item is already in container (Key: %v)", fields[0]));
    }

    return statistics;
}

std::optional<i64> ReadMemoryLimit(const TString& path)
{
    try {
        if (!NFs::Exists(path)) {
            return std::nullopt;
        }
        auto rawValue = TFileInput(path).ReadLine();
        if (rawValue == "max") {
            return std::nullopt;
        }
        return FromString<i64>(rawValue);
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TV1ControllerInfo
{
    std::string MountName;
    std::string CGroupPath;
};

void FormatValue(TStringBuilderBase* builder, const TV1ControllerInfo& value, TStringBuf /*spec*/)
{
    builder->AppendString(GetCGroupFilePath(value.MountName, value.CGroupPath));
}

////////////////////////////////////////////////////////////////////////////////

class TV1CGroupStatisticsFetcher
    : public ICGroupStatisticsFetcher
{
public:
    explicit TV1CGroupStatisticsFetcher(THashMap<std::string, TV1ControllerInfo> controllers)
        : Controllers_(std::move(controllers))
    {
        YT_LOG_INFO("CGroups statistics fetcher initialized (IsV2: false, Controllers: %v)", Controllers_);
    }

    bool IsV2() const override
    {
        return false;
    }

    TMemoryStatistics GetMemoryStatistics() const override
    {
        auto statistics = ReadAndParseStatFile(GetStatPath("memory", "memory.stat"));

        return TMemoryStatistics{
            .ResidentAnon = statistics["total_rss"],
            .TmpfsUsage = statistics["total_shmem"],
            .MappedFile = statistics["total_mapped_file"],
            .MajorPageFaults = statistics["total_pgmajfault"],
            .Cache = statistics["total_cache"],
            .RssHuge = statistics["total_rss_huge"],
            .Dirty = statistics["total_dirty"],
            .Writeback = statistics["total_writeback"],
        };
    }

    TMemoryLimits GetMemoryLimits() const override
    {
        auto statistics = ReadAndParseStatFile(GetStatPath("memory", "memory.stat"));
        auto* memoryLimit = statistics.FindPtr("hierarchical_memory_limit");

        return TMemoryLimits{
            .MemoryLimit = memoryLimit ? std::optional(*memoryLimit) : std::nullopt,
            .AnonymousMemoryLimit = ReadMemoryLimit(GetStatPath("memory", "memory.anon.limit")),
        };
    }

    TCpuStatistics GetCpuStatistics() const override
    {
        auto cpuAcctStatistics = ReadAndParseStatFile(GetStatPath("cpuacct", "cpuacct.stat"));

        i64 ticksPerSecond;
#if defined(__linux__)
        ticksPerSecond = sysconf(_SC_CLK_TCK);
#else
        ticksPerSecond = 1'000'000'000;
#endif

        auto fromJiffies = [&] (i64 jiffies) {
            return TDuration::MicroSeconds(1'000'000 * jiffies / ticksPerSecond);
        };

        return TCpuStatistics{
            .UserTime = fromJiffies(cpuAcctStatistics["user"]),
            .SystemTime = fromJiffies(cpuAcctStatistics["system"]),
        };
    }

    TCpuThrottlingStatistics GetCpuThrottlingStatistics() const override
    {
        auto cpuStatistics = ReadAndParseStatFile(GetStatPath("cpu", "cpu.stat"));

        return TCpuThrottlingStatistics{
            .NrPeriods = static_cast<ui64>(cpuStatistics["nr_periods"]),
            .NrThrottled = static_cast<ui64>(cpuStatistics["nr_throttled"]),
            .ThrottledTime = TDuration::MicroSeconds(cpuStatistics["h_throttled_time"] / 1000),
            .WaitTime = ReadCpuWaitTime(),
        };
    }

    TBlockIOStatistics GetBlockIOStatistics() const override
    {
        auto it = Controllers_.find("blkio");
        auto cgroupPath = it != Controllers_.end()
            ? Format("/%v/%v", it->second.MountName, it->second.CGroupPath)
            : TString("/blkio");

        TBlockIOStatistics statistics;

        for (auto fileName : {"blkio.io_service_bytes_recursive", "blkio.throttle.io_service_bytes_recursive"}) {
            auto filePath = GetCGroupFilePath(cgroupPath, fileName);
            if (!NFs::Exists(filePath)) {
                continue;
            }

            auto blkioStatistics = ReadAndParseBlkIOStatFile(filePath);
            statistics.IOReadByte += blkioStatistics["Read"];
            statistics.IOWriteByte += blkioStatistics["Write"];
            break;
        }

        for (auto fileName : {"blkio.io_serviced_recursive", "blkio.throttle.io_serviced_recursive"}) {
            auto filePath = GetCGroupFilePath(cgroupPath, fileName);
            if (!NFs::Exists(filePath)) {
                continue;
            }

            auto blkioStatistics = ReadAndParseBlkIOStatFile(filePath);
            statistics.IOReadOps += blkioStatistics["Read"];
            statistics.IOWriteOps += blkioStatistics["Write"];
            break;
        }

        return statistics;
    }

    i64 GetOomKillCount() const override
    {
        auto oomPath = GetStatPath("memory", "memory.oom_control");
        if (!NFs::Exists(oomPath)) {
            return 0;
        }
        auto statistics = ReadAndParseStatFile(oomPath);
        return statistics["oom_kill"];
    }

private:
    THashMap<std::string, TV1ControllerInfo> Controllers_;

    TString GetStatPath(const std::string& controller, const std::string& fileName) const
    {
        auto it = Controllers_.find(controller);
        if (it == Controllers_.end()) {
            return GetCGroupFilePath(Format("/%v", controller), fileName);
        }
        return GetCGroupFilePath(Format("/%v/%v", it->second.MountName, it->second.CGroupPath), fileName);
    }

    // cpuacct.wait is a single value in thread-nanoseconds (kernel extension).
    TDuration ReadCpuWaitTime() const
    {
        auto path = GetStatPath("cpuacct", "cpuacct.wait");
        try {
            if (NFs::Exists(path)) {
                auto ns = FromString<ui64>(TFileInput(path).ReadLine());
                return TDuration::MicroSeconds(ns / 1000);
            }
        } catch (const std::exception&) {
        }

        return TDuration::Zero();
    }

    static THashMap<std::string, i64> ReadAndParseBlkIOStatFile(const TString& fileName)
    {
        THashMap<std::string, i64> statistics;

        auto rawStatFile = TFileInput(fileName).ReadAll();
        for (const auto& line : SplitString(rawStatFile, "\n")) {
            auto fields = SplitString(line, " ");
            if (fields.size() != 3) {
                continue;
            }

            statistics[fields[1]] += FromString<i64>(fields[2]);
        }

        return statistics;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TV2CGroupStatisticsFetcher
    : public ICGroupStatisticsFetcher
{
public:
    explicit TV2CGroupStatisticsFetcher(std::string cgroupPath)
        : CGroupPath_(std::move(cgroupPath))
    {
        YT_LOG_INFO("CGroups statistics fetcher initialized (IsV2: true, CGroupPath: %v)", GetCGroupFilePath(CGroupPath_, ""));
    }

    bool IsV2() const override
    {
        return true;
    }

    TMemoryStatistics GetMemoryStatistics() const override
    {
        auto statistics = ReadAndParseStatFile(GetCGroupFilePath(CGroupPath_, "memory.stat"));

        return TMemoryStatistics{
            .ResidentAnon = statistics["anon"],
            .TmpfsUsage = statistics["shmem"],
            .MappedFile = statistics["mapped_file"],
            .MajorPageFaults = statistics["pgmajfault"],
            .Cache = statistics["file"],
            .RssHuge = statistics["anon_thp"],
            .Dirty = statistics["file_dirty"],
            .Writeback = statistics["file_writeback"],
        };
    }

    TMemoryLimits GetMemoryLimits() const override
    {
        return TMemoryLimits{
            .MemoryLimit = ReadMemoryLimit(GetCGroupFilePath(CGroupPath_, "memory.max")),
            .AnonymousMemoryLimit = ReadMemoryLimit(GetCGroupFilePath(CGroupPath_, "memory.anon.limit")),
        };
    }

    TCpuStatistics GetCpuStatistics() const override
    {
        auto cpuStatistics = ReadAndParseStatFile(GetCGroupFilePath(CGroupPath_, "cpu.stat"));

        return TCpuStatistics{
            .UserTime = TDuration::MicroSeconds(cpuStatistics["user_usec"]),
            .SystemTime = TDuration::MicroSeconds(cpuStatistics["system_usec"]),
        };
    }

    TCpuThrottlingStatistics GetCpuThrottlingStatistics() const override
    {
        auto cpuStatistics = ReadAndParseStatFile(GetCGroupFilePath(CGroupPath_, "cpu.stat"));

        return TCpuThrottlingStatistics{
            .NrPeriods = static_cast<ui64>(cpuStatistics["nr_periods"]),
            .NrThrottled = static_cast<ui64>(cpuStatistics["nr_throttled"]),
            .ThrottledTime = TDuration::MicroSeconds(cpuStatistics["h_throttled_usec"]),
            .WaitTime = ReadCpuWaitTime(),
        };
    }

    TBlockIOStatistics GetBlockIOStatistics() const override
    {
        auto ioStatPath = GetCGroupFilePath(CGroupPath_, "io.stat");
        if (!NFs::Exists(ioStatPath)) {
            return {};
        }
        auto ioStatistics = ReadAndParseIOStatFile(ioStatPath);

        return TBlockIOStatistics{
            .IOReadByte = ioStatistics["rbytes"],
            .IOWriteByte = ioStatistics["wbytes"],
            .IOReadOps = ioStatistics["rios"],
            .IOWriteOps = ioStatistics["wios"],
        };
    }

    i64 GetOomKillCount() const override
    {
        auto oomPath = GetCGroupFilePath(CGroupPath_, "memory.events");
        if (!NFs::Exists(oomPath)) {
            return 0;
        }
        auto statistics = ReadAndParseStatFile(oomPath);
        return statistics["oom_kill"];
    }

private:
    const std::string CGroupPath_;

    // Tries cpuacct.wait (kernel extension, nanoseconds),
    // falls back to PSI cpu.pressure (upstream kernel, microseconds).
    // NB(pavook): we prefer cpuacct.wait because it is an honest sum of
    // individual thread wait times, not the union of those wait times ("pressure").
    TDuration ReadCpuWaitTime() const
    {
        auto cpuacctWaitPath = GetCGroupFilePath(CGroupPath_, "cpuacct.wait");
        try {
            if (NFs::Exists(cpuacctWaitPath)) {
                auto ns = FromString<ui64>(TFileInput(cpuacctWaitPath).ReadLine());
                return TDuration::MicroSeconds(ns / 1000);
            }
        } catch (const std::exception&) {
        }

        auto cpuPressurePath = GetCGroupFilePath(CGroupPath_, "cpu.pressure");
        try {
            auto raw = TFileInput(cpuPressurePath).ReadAll();
            for (const auto& line : SplitString(raw, "\n")) {
                if (!line.StartsWith("some ")) {
                    continue;
                }
                for (auto token : StringSplitter(line).Split(' ')) {
                    TStringBuf buf(token);
                    if (buf.SkipPrefix("total=")) {
                        return TDuration::MicroSeconds(FromString<ui64>(buf));
                    }
                }
            }
        } catch (const std::exception&) {
        }

        return TDuration::Zero();
    }

    static THashMap<std::string, i64> ReadAndParseIOStatFile(const TString& fileName)
    {
        THashMap<std::string, i64> statistics;

        auto rawStatFile = TFileInput(fileName).ReadAll();
        for (auto line : SplitString(rawStatFile, "\n")) {
            auto fields = SplitString(line, " ");
            for (int index = 1; index < std::ssize(fields); ++index) {
                auto tokens = SplitString(fields[index], "=");
                if (tokens.size() != 2) {
                    continue;
                }
                statistics[tokens[0]] += FromString<i64>(tokens[1]);
            }
        }

        return statistics;
    }
};

////////////////////////////////////////////////////////////////////////////////

bool DetectIsV2()
{
    struct statfs sfs;
    if (statfs("/sys/fs/cgroup", &sfs) != 0) {
        YT_LOG_WARNING("Failed to statfs /sys/fs/cgroup, assuming cgroup v1");
        return false;
    }

    if (sfs.f_type == CGROUP2_SUPER_MAGIC) {
        return true;
    }

    // On v1 systems, /sys/fs/cgroup should be a tmpfs with per-controller mounts underneath.
    if (sfs.f_type != TMPFS_MAGIC) {
        YT_LOG_ERROR(
            "Unexpected filesystem type on /sys/fs/cgroup (FsType: 0x%x), assuming cgroup v1",
            sfs.f_type);
    }

    return false;
}

// Parses /proc/self/cgroup to extract the cgroup path for the current process.
// For v2: returns the unified cgroup path from the "0::" line.
// For v1: returns per-controller mount names and paths.
std::unique_ptr<ICGroupStatisticsFetcher> CreateFetcher()
{
    bool isV2 = DetectIsV2();

    TString rawSelfCGroups;
    try {
        rawSelfCGroups = TFileInput("/proc/self/cgroup").ReadAll();
    } catch (const std::exception& ex) {
        YT_LOG_WARNING("Failed to read /proc/self/cgroup (Error: %v)", ex.what());
    }

    if (isV2) {
        // Extract the unified cgroup path from the "0::<path>" line.
        for (const auto& line : SplitString(rawSelfCGroups, "\n")) {
            std::vector<std::string> tokens = StringSplitter(line).Split(':').Limit(3);
            if (tokens.size() < 3 || !tokens[1].empty()) {
                continue;
            }

            // NB: There are issues with cgroup namespaces in Kubernetes
            // (see https://github.com/kubernetes/enhancements/pull/1370),
            // where /proc/self/cgroup contains real cgroup path
            // but in /sys/fs/cgroup it is just root cgroup.
            auto cgroupPath = tokens[2];
            if (!NFs::Exists(GetCGroupFilePath(cgroupPath, "memory.stat"))) {
                cgroupPath = "/";
            }

            return std::make_unique<TV2CGroupStatisticsFetcher>(std::move(cgroupPath));
        }

        YT_LOG_WARNING("Failed to find v2 cgroup path in /proc/self/cgroup, falling back to root");
        return std::make_unique<TV2CGroupStatisticsFetcher>("/");
    }

    // V1: parse per-controller entries.
    THashMap<std::string, TV1ControllerInfo> v1Controllers;

    for (const auto& line : SplitString(rawSelfCGroups, "\n")) {
        std::vector<std::string> tokens = StringSplitter(line).Split(':').Limit(3);
        if (tokens.size() < 3 || tokens[1].empty()) {
            continue;
        }

        const auto& cgroupType = tokens[1];
        const auto& cgroup = tokens[2];

        for (const auto& controller : StringSplitter(cgroupType).Split(',')) {
            v1Controllers[controller] = TV1ControllerInfo{
                .MountName = cgroupType,
                .CGroupPath = cgroup,
            };
        }
    }

    if (v1Controllers.empty()) {
        YT_LOG_WARNING("Failed to parse /proc/self/cgroup, no v1 controllers found");
    }

    return std::make_unique<TV1CGroupStatisticsFetcher>(std::move(v1Controllers));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

const ICGroupStatisticsFetcher* TSelfCGroupsStatisticsFetcher::Get()
{
    static auto impl = CreateFetcher();
    return impl.get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCGroups
