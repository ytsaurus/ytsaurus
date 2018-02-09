#include "cgroup.h"
#include "private.h"

#include <yt/core/misc/fs.h>
#include <yt/core/misc/proc.h>
#include <yt/core/tools/registry.h>
#include <yt/core/tools/tools.h>

#include <yt/core/ytree/fluent.h>

#include <util/string/split.h>
#include <util/system/filemap.h>

#include <util/system/yield.h>

#ifdef _linux_
    #include <unistd.h>
    #include <sys/stat.h>
    #include <errno.h>
#endif

namespace NYT {
namespace NCGroup {

using namespace NTools;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CGroupLogger;
static const TString CGroupRootPath("/sys/fs/cgroup");
#ifdef _linux_
static const int ReadByAll = S_IRUSR | S_IRGRP | S_IROTH;
static const int ReadExecuteByAll = ReadByAll | S_IXUSR | S_IXGRP | S_IXOTH;
#endif

////////////////////////////////////////////////////////////////////////////////

namespace {

TString GetParentFor(const TString& type)
{
#ifdef _linux_
    auto rawData = TUnbufferedFileInput("/proc/self/cgroup").ReadAll();
    auto result = ParseProcessCGroups(rawData);
    return result[type];
#else
    return "_parent_";
#endif
}

#ifdef _linux_

std::vector<TString> ReadAllValues(const TString& fileName)
{
    auto raw = TUnbufferedFileInput(fileName).ReadAll();
    LOG_DEBUG(
        "File %v contains %Qv",
        fileName,
        raw);

    TVector<TString> values;
    Split(raw.data(), " \n", values);
    return std::vector<TString>(values.begin(), values.end());
}

TDuration FromJiffies(ui64 jiffies)
{
    static long ticksPerSecond = sysconf(_SC_CLK_TCK);
    return TDuration::MicroSeconds(1000 * 1000 * jiffies / ticksPerSecond);
}

#endif

} // namespace

////////////////////////////////////////////////////////////////////////////////

void RunKiller(const TString& processGroupPath)
{
    LOG_INFO("Killing processes in cgroup %v", processGroupPath);

#ifdef _linux_
    TNonOwningCGroup group(processGroupPath);
    if (group.IsNull()) {
        return;
    }

    if (!group.Exists()) {
        LOG_WARNING("Cgroup %v does not exists: stop killer", processGroupPath);
        return;
    }

    group.Lock();

    auto children = group.GetChildren();
    auto pids = group.GetTasks();
    if (children.empty() && pids.empty())
        return;

    RunTool<TKillProcessGroupTool>(processGroupPath);
#endif
}

void TKillProcessGroupTool::operator()(const TString& processGroupPath) const
{
    SafeSetUid(0);
    NCGroup::TNonOwningCGroup group(processGroupPath);
    group.Kill();
}

////////////////////////////////////////////////////////////////////////////////

TNonOwningCGroup::TNonOwningCGroup(const TString& fullPath)
    : FullPath_(fullPath)
{ }

TNonOwningCGroup::TNonOwningCGroup(const TString& type, const TString& name)
    : FullPath_(NFS::CombinePaths({
        CGroupRootPath,
        type,
        GetParentFor(type),
        name}))
{ }

TNonOwningCGroup::TNonOwningCGroup(TNonOwningCGroup&& other)
    : FullPath_(std::move(other.FullPath_))
{ }

void TNonOwningCGroup::AddTask(int pid) const
{
    LOG_INFO(
        "Adding %v to cgroup %v",
        pid,
        FullPath_);
    Append("tasks", ToString(pid));
}

void TNonOwningCGroup::AddCurrentTask() const
{
    YCHECK(!IsNull());
#ifdef _linux_
    auto pid = getpid();
    AddTask(pid);
#endif
}

TString TNonOwningCGroup::Get(const TString& name) const
{
    YCHECK(!IsNull());
    TString result;
#ifdef _linux_
    const auto path = GetPath(name);
    result = TFileInput(path).ReadLine();
#endif
    return result;
}

void TNonOwningCGroup::Set(const TString& name, const TString& value) const
{
    YCHECK(!IsNull());
#ifdef _linux_
    auto path = GetPath(name);
    TUnbufferedFileOutput output(TFile(path, EOpenModeFlag::WrOnly));
    output << value;
#endif
}

void TNonOwningCGroup::Append(const TString& name, const TString& value) const
{
    YCHECK(!IsNull());
#ifdef _linux_
    auto path = GetPath(name);
    TUnbufferedFileOutput output(TFile(path, EOpenModeFlag::ForAppend));
    output << value;
#endif
}

bool TNonOwningCGroup::IsRoot() const
{
    return FullPath_ == CGroupRootPath;
}

bool TNonOwningCGroup::IsNull() const
{
    return FullPath_.Empty();
}

bool TNonOwningCGroup::Exists() const
{
    return NFS::Exists(FullPath_);
}

std::vector<int> TNonOwningCGroup::GetTasks() const
{
    std::vector<int> results;
    if (!IsNull()) {
#ifdef _linux_
        auto values = ReadAllValues(GetPath("tasks"));
        for (const auto& value : values) {
            int pid = FromString<int>(value);
            results.push_back(pid);
        }
#endif
    }
    return results;
}

const TString& TNonOwningCGroup::GetFullPath() const
{
    return FullPath_;
}

std::vector<TNonOwningCGroup> TNonOwningCGroup::GetChildren() const
{
    // We retry enumerating directories, since it may fail with weird diagnostics if
    // number of subcgroups changes.
    while (true) {
        try {
            std::vector<TNonOwningCGroup> result;

            if (IsNull()) {
                return result;
            }

            auto directories = NFS::EnumerateDirectories(FullPath_);
            for (const auto& directory : directories) {
                result.emplace_back(NFS::CombinePaths(FullPath_, directory));
            }
            return result;
        } catch (const std::exception& ex) {
            LOG_WARNING(ex, "Failed to list subcgroups (Path: %v)", FullPath_);
        }
    }
}

void TNonOwningCGroup::EnsureExistance() const
{
    LOG_INFO("Creating cgroup %v", FullPath_);

    YCHECK(!IsNull());

#ifdef _linux_
    NFS::MakeDirRecursive(FullPath_, 0755);
#endif
}

void TNonOwningCGroup::Lock() const
{
    Traverse(
        BIND([] (const TNonOwningCGroup& group) { group.DoLock(); }),
        BIND([] (const TNonOwningCGroup& group) {})
    );
}

void TNonOwningCGroup::Unlock() const
{
    Traverse(
        BIND([] (const TNonOwningCGroup& group) {}),
        BIND([] (const TNonOwningCGroup& group) { group.DoUnlock(); })
    );
}

void TNonOwningCGroup::Kill() const
{
    YCHECK(!IsRoot());

    Traverse(
        BIND([] (const TNonOwningCGroup& group) { group.DoKill(); }),
        BIND([] (const TNonOwningCGroup& group) {})
    );
}

void TNonOwningCGroup::RemoveAllSubcgroups() const
{
    auto this_ = this;
    Traverse(
        BIND([] (const TNonOwningCGroup& group) {
            group.TryUnlock();
        }),
        BIND([this_] (const TNonOwningCGroup& group) {
            if (this_ != &group) {
                group.DoRemove();
            }
        })
    );
}

void TNonOwningCGroup::RemoveRecursive() const
{
    RemoveAllSubcgroups();
    DoRemove();
}

void TNonOwningCGroup::DoLock() const
{
    LOG_INFO("Locking cgroup %v", FullPath_);

#ifdef _linux_
    if (!IsNull()) {
        int code = chmod(~FullPath_, ReadExecuteByAll);
        YCHECK(code == 0);

        code = chmod(~GetPath("tasks"), ReadByAll);
        YCHECK(code == 0);
    }
#endif
}

bool TNonOwningCGroup::TryUnlock() const
{
    LOG_INFO("Unlocking cgroup %v", FullPath_);

    if (!Exists()) {
        return true;
    }

    bool result = true;

#ifdef _linux_
    if (!IsNull()) {
        int code = chmod(~GetPath("tasks"), ReadByAll | S_IWUSR);
        if (code != 0) {
            result = false;
        }

        code = chmod(~FullPath_, ReadExecuteByAll | S_IWUSR);
        if (code != 0) {
            result = false;
        }
    }
#endif

    return result;
}

void TNonOwningCGroup::DoUnlock() const
{
    YCHECK(TryUnlock());
}

void TNonOwningCGroup::DoKill() const
{
    LOG_DEBUG("Started killing processes in cgroup %v", FullPath_);

#ifdef _linux_
    while (true) {
        auto pids = GetTasks();
        if (pids.empty())
            break;

        LOG_DEBUG("Killing processes (Pids: %v)", pids);

        for (int pid : pids) {
            auto result = kill(pid, SIGKILL);
            if (result == -1) {
                YCHECK(errno == ESRCH);
            }
        }

        ThreadYield();
    }
#endif

    LOG_DEBUG("Finished killing processes in cgroup %v", FullPath_);
}

void TNonOwningCGroup::DoRemove() const
{
    if (NFS::Exists(FullPath_)) {
        NFS::Remove(FullPath_);
    }
}

void TNonOwningCGroup::Traverse(
    const TCallback<void(const TNonOwningCGroup&)>& preorderAction,
    const TCallback<void(const TNonOwningCGroup&)>& postorderAction) const
{
    preorderAction.Run(*this);

    for (const auto& child : GetChildren()) {
        child.Traverse(preorderAction, postorderAction);
    }

    postorderAction.Run(*this);
}

TString TNonOwningCGroup::GetPath(const TString& filename) const
{
    return NFS::CombinePaths(FullPath_, filename);
}

////////////////////////////////////////////////////////////////////////////////

TCGroup::TCGroup(const TString& type, const TString& name)
    : TNonOwningCGroup(type, name)
{ }

TCGroup::TCGroup(TCGroup&& other)
    : TNonOwningCGroup(std::move(other))
    , Created_(other.Created_)
{
    other.Created_ = false;
}

TCGroup::TCGroup(TNonOwningCGroup&& other)
    : TNonOwningCGroup(std::move(other))
    , Created_(false)
{ }

TCGroup::~TCGroup()
{
    if (Created_) {
        Destroy();
    }
}

void TCGroup::Create()
{
    EnsureExistance();
    Created_ = true;
}

void TCGroup::Destroy()
{
    LOG_INFO("Destroying cgroup %v", FullPath_);
    YCHECK(Created_);

#ifdef _linux_
    try {
        NFS::Remove(FullPath_);
    } catch (const std::exception& ex) {
        LOG_FATAL(ex, "Failed to destroy cgroup %v", FullPath_);
    }
#endif
    Created_ = false;
}

bool TCGroup::IsCreated() const
{
    return Created_;
}

////////////////////////////////////////////////////////////////////////////////

const TString TCpuAccounting::Name = "cpuacct";

TCpuAccounting::TStatistics& operator-=(TCpuAccounting::TStatistics& lhs, const TCpuAccounting::TStatistics& rhs)
{
    lhs.UserTime = lhs.UserTime > rhs.UserTime
        ? lhs.UserTime - rhs.UserTime
        : TDuration::Zero();

    lhs.SystemTime = lhs.SystemTime > rhs.SystemTime
        ? lhs.SystemTime - rhs.SystemTime
        : TDuration::Zero();

    return lhs;
}

TCpuAccounting::TCpuAccounting(const TString& name)
    : TCGroup(Name, name)
{ }

TCpuAccounting::TCpuAccounting(TNonOwningCGroup&& nonOwningCGroup)
    : TCGroup(std::move(nonOwningCGroup))
{ }

TCpuAccounting::TStatistics TCpuAccounting::GetStatisticsRecursive() const
{
    TCpuAccounting::TStatistics result;
#ifdef _linux_
    try {
        auto path = NFS::CombinePaths(GetFullPath(), "cpuacct.stat");
        auto values = ReadAllValues(path);
        YCHECK(values.size() == 4);

        TString type[2];
        ui64 jiffies[2];

        for (int i = 0; i < 2; ++i) {
            type[i] = values[2 * i];
            jiffies[i] = FromString<ui64>(values[2 * i + 1]);
        }

        for (int i = 0; i < 2; ++i) {
            if (type[i] == "user") {
                result.UserTime = FromJiffies(jiffies[i]);
            } else if (type[i] == "system") {
                result.SystemTime = FromJiffies(jiffies[i]);
            }
        }
    } catch (const std::exception& ex) {
        LOG_FATAL(
            ex,
            "Failed to retreive CPU statistics from cgroup %v",
            GetFullPath());
    }
#endif
    return result;
}

TCpuAccounting::TStatistics TCpuAccounting::GetStatistics() const
{
    auto statistics = GetStatisticsRecursive();

    for (auto& cgroup : GetChildren()) {
        auto cpuCGroup = TCpuAccounting(std::move(cgroup));
        statistics -= cpuCGroup.GetStatisticsRecursive();
    }

    return statistics;
}

void Serialize(const TCpuAccounting::TStatistics& statistics, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("user").Value(statistics.UserTime.MilliSeconds())
            .Item("system").Value(statistics.SystemTime.MilliSeconds())
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

const TString TCpu::Name = "cpu";

static const int DefaultCpuShare = 1024;

TCpu::TCpu(const TString& name)
    : TCGroup(Name, name)
{ }

void TCpu::SetShare(double share)
{
    int cpuShare = static_cast<int>(share * DefaultCpuShare);
    Set("cpu.shares", ToString(cpuShare));
}

////////////////////////////////////////////////////////////////////////////////

const TString TBlockIO::Name = "blkio";

TBlockIO::TBlockIO(const TString& name)
    : TCGroup(Name, name)
{ }

// For more information about format of data
// read https://www.kernel.org/doc/Documentation/cgroups/blkio-controller.txt

TBlockIO::TStatistics TBlockIO::GetStatistics() const
{
    TBlockIO::TStatistics result;
#ifdef _linux_
        auto bytesStats = GetDetailedStatistics("blkio.io_service_bytes");
        for (const auto& item : bytesStats) {
            if (item.Type == "Read") {
                result.BytesRead += item.Value;
            } else if (item.Type == "Write") {
                result.BytesWritten += item.Value;
            }
        }

        auto ioStats = GetDetailedStatistics("blkio.io_serviced");
        for (const auto& item : ioStats) {
            if (item.Type == "Read") {
                result.IORead += item.Value;
                result.IOTotal += item.Value;
            } else if (item.Type == "Write") {
                result.IOWrite += item.Value;
                result.IOTotal += item.Value;
            }
        }
#endif
    return result;
}

std::vector<TBlockIO::TStatisticsItem> TBlockIO::GetIOServiceBytes() const
{
    return GetDetailedStatistics("blkio.io_service_bytes");
}

std::vector<TBlockIO::TStatisticsItem> TBlockIO::GetIOServiced() const
{
    return GetDetailedStatistics("blkio.io_serviced");
}

std::vector<TBlockIO::TStatisticsItem> TBlockIO::GetDetailedStatistics(const char* filename) const
{
    std::vector<TBlockIO::TStatisticsItem> result;
#ifdef _linux_
    try {
        auto path = NFS::CombinePaths(GetFullPath(), filename);
        auto values = ReadAllValues(path);

        int lineNumber = 0;
        while (3 * lineNumber + 2 < values.size()) {
            TStatisticsItem item;
            item.DeviceId = values[3 * lineNumber];
            item.Type = values[3 * lineNumber + 1];
            item.Value = FromString<ui64>(values[3 * lineNumber + 2]);

            {
                auto guard = Guard(SpinLock_);
                DeviceIds_.insert(item.DeviceId);
            }

            if (item.Type == "Read" || item.Type == "Write") {
                result.push_back(item);

                LOG_DEBUG("IO operations serviced (OperationCount: %v, OperationType: %v, DeviceId: %v)",
                    item.Value,
                    item.Type,
                    item.DeviceId);
            }
            ++lineNumber;
        }
    } catch (const std::exception& ex) {
        LOG_FATAL(
            ex,
            "Failed to retreive block IO statistics from cgroup %v",
            GetFullPath());
    }
#endif
    return result;
}

void TBlockIO::ThrottleOperations(i64 operations) const
{
    auto guard = Guard(SpinLock_);
    for (const auto& deviceId : DeviceIds_) {
        auto value = Format("%v %v", deviceId, operations);
        Append("blkio.throttle.read_iops_device", value);
        Append("blkio.throttle.write_iops_device", value);
    }
}

void Serialize(const TBlockIO::TStatistics& statistics, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("bytes_read").Value(statistics.BytesRead)
            .Item("bytes_written").Value(statistics.BytesWritten)
            .Item("io_read").Value(statistics.IORead)
            .Item("io_write").Value(statistics.IOWrite)
            .Item("io_total").Value(statistics.IOTotal)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

const TString TMemory::Name = "memory";

TMemory::TMemory(const TString& name)
    : TCGroup(Name, name)
{ }

TMemory::TStatistics TMemory::GetStatistics() const
{
    TMemory::TStatistics result;
#ifdef _linux_
     try {
        auto values = ReadAllValues(GetPath("memory.stat"));
        int lineNumber = 0;
        while (2 * lineNumber + 1 < values.size()) {
            const auto& type = values[2 * lineNumber];
            const auto& unparsedValue = values[2 * lineNumber + 1];
            if (type == "rss") {
                result.Rss = FromString<ui64>(unparsedValue);
            }
            if (type == "mapped_file") {
                result.MappedFile = FromString<ui64>(unparsedValue);
            }
            if (type == "pgmajfault") {
                result.MajorPageFaults = FromString<ui64>(unparsedValue);
            }
            ++lineNumber;
        }
    } catch (const std::exception& ex) {
        LOG_FATAL(
            ex,
            "Failed to retreive memory statistics from cgroup %v",
            GetFullPath());
    }
#endif
    return result;
}

i64 TMemory::GetMaxMemoryUsage() const
{
    return FromString<i64>(Get("memory.max_usage_in_bytes"));
}

void TMemory::SetLimitInBytes(i64 bytes) const
{
    Set("memory.limit_in_bytes", ToString(bytes));
}

void TMemory::ForceEmpty() const
{
    Set("memory.force_empty", "0");
}

void Serialize(const TMemory::TStatistics& statistics, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("rss").Value(statistics.Rss)
            .Item("mapped_file").Value(statistics.MappedFile)
            .Item("major_page_faults").Value(statistics.MajorPageFaults)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

const TString TFreezer::Name = "freezer";

TFreezer::TFreezer(const TString& name)
    : TCGroup(Name, name)
{ }

TString TFreezer::GetState() const
{
    return Get("freezer.state");
}

void TFreezer::Freeze() const
{
    Set("freezer.state", "FROZEN");
}

void TFreezer::Unfreeze() const
{
    Set("freezer.state", "THAWED");
}

////////////////////////////////////////////////////////////////////////////////

std::map<TString, TString> ParseProcessCGroups(const TString& str)
{
    std::map<TString, TString> result;

    TVector<TString> values;
    Split(str.data(), ":\n", values);
    for (size_t i = 0; i + 2 < values.size(); i += 3) {
        FromString<int>(values[i]);

        const TString& subsystemsSet = values[i + 1];
        const TString& name = values[i + 2];

        TVector<TString> subsystems;
        Split(subsystemsSet.data(), ",", subsystems);
        for (const auto& subsystem : subsystems) {
            if (!subsystem.StartsWith("name=")) {
                int start = 0;
                if (name.StartsWith("/")) {
                    start = 1;
                }
                result[subsystem] = name.substr(start);
            }
        }
    }

    return result;
}

bool IsValidCGroupType(const TString& type)
{
    if (type == TCpuAccounting::Name) {
        return true;
    }
    if (type == TCpu::Name) {
        return true;
    }
    if (type == TBlockIO::Name) {
        return true;
    }
    if (type == TMemory::Name) {
        return true;
    }
    if (type == TFreezer::Name) {
        return true;
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCGroup
} // namespace NYT
