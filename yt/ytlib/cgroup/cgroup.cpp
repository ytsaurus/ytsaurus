#include "stdafx.h"
#include "private.h"
#include "cgroup.h"

#include <core/misc/fs.h>
#include <core/misc/error.h>

#include <util/system/fs.h>
#include <util/string/split.h>
#include <util/string/strip.h>
#include <util/folder/path.h>

#ifdef _linux_
  #include <unistd.h>
  #include <sys/eventfd.h>
#endif

namespace NYT {
namespace NCGroup {

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = CGroupLogger;
static const char* CGroupRootPath = "/sys/fs/cgroup";
static const int InvalidFd = -1;

////////////////////////////////////////////////////////////////////////////////

namespace {

Stroka GetParentFor(const Stroka& type)
{
    auto rawData = TFileInput("/proc/self/cgroup").ReadAll();
    auto result = ParseCurrentProcessCGroups(TStringBuf(rawData.data(), rawData.size()));
    return result[type];
}


yvector<Stroka> ReadAllValues(const Stroka& filename)
{
    auto raw = TFileInput(filename).ReadAll();
    LOG_DEBUG("File %s contains: %s", ~filename.Quote(), ~raw);

    yvector<Stroka> values;
    Split(raw.data(), " \n", values);
    return values;
}

}

////////////////////////////////////////////////////////////////////////////////

TEvent::TEvent(int eventFd, int fd)
    : EventFd_(eventFd)
    , Fd_(fd)
    , Fired_(false)
{ }

TEvent::TEvent()
    : TEvent(InvalidFd, InvalidFd)
{ }

TEvent::TEvent(TEvent&& other)
    : TEvent()
{
    Swap(other);
}

TEvent::~TEvent()
{
    Destroy();
}

TEvent& TEvent::operator=(TEvent&& other)
{
    if (this == &other) {
        return *this;
    }
    Destroy();
    Swap(other);
    return *this;
}

bool TEvent::Fired()
{
    YCHECK(EventFd_ != InvalidFd);

    if (Fired_) {
        return true;
    }

    auto bytesRead = ::read(EventFd_, &LastValue_, sizeof(LastValue_));

    if (bytesRead < 0) {
        if (errno == EWOULDBLOCK || errno == EAGAIN) {
            return false;
        }
        THROW_ERROR_EXCEPTION() << TError::FromSystem();
    }
    YCHECK(bytesRead == sizeof(LastValue_));
    Fired_ = true;
    return true;
}

void TEvent::Clear()
{
    Fired_ = false;
}

void TEvent::Destroy()
{
    Clear();
    if (EventFd_ != InvalidFd) {
        ::close(EventFd_);
    }
    EventFd_ = InvalidFd;

    if (Fd_ != InvalidFd) {
        ::close(Fd_);
    }
    Fd_ = InvalidFd;
}

i64 TEvent::GetLastValue() const
{
    return LastValue_;
}

void TEvent::Swap(TEvent& other)
{
    std::swap(EventFd_, other.EventFd_);
    std::swap(Fd_, other.Fd_);
    std::swap(Fired_, other.Fired_);
}

////////////////////////////////////////////////////////////////////////////////

std::vector<Stroka> GetSupportedCGroups()
{
    std::vector<Stroka> result;
    result.push_back("cpuacct");
    result.push_back("blkio");
    result.push_back("memory");
    return result;
}

void RemoveAllSubscgroupsImpl(const TFsPath& path)
{
    if (path.Exists()) {
        yvector<TFsPath> children;
        path.List(children);
        for (const auto& child : children) {
            if (child.IsDirectory()) {
                RemoveAllSubscgroupsImpl(child);
                child.DeleteIfExists();
            }
        }
    }
}

void RemoveAllSubcgroups(const Stroka& path)
{
    RemoveAllSubscgroupsImpl(TFsPath(path));
}

////////////////////////////////////////////////////////////////////////////////

TNonOwningCGroup::TNonOwningCGroup(const Stroka& fullPath)
    : FullPath_(fullPath)
{ }

TNonOwningCGroup::TNonOwningCGroup(const Stroka& type, const Stroka& name)
    : FullPath_(NFS::CombinePaths(NFS::CombinePaths(NFS::CombinePaths(CGroupRootPath,  type), GetParentFor(type)), name))
{ }

TNonOwningCGroup::TNonOwningCGroup(TNonOwningCGroup&& other)
    : FullPath_(std::move(other.FullPath_))
{ }

// This method SHOULD work fine in forked process
// So we cannot use out logging|profiling framework
void TNonOwningCGroup::AddCurrentTask()
{
#ifdef _linux_
    auto pid = getpid();

    auto path = NFS::CombinePaths(FullPath_, "tasks");
    TFileOutput output(TFile(path, OpenMode::ForAppend));
    output << pid;
#endif
}

void TNonOwningCGroup::Set(const Stroka& name, const Stroka& value) const
{
#ifdef _linux_
    auto path = NFS::CombinePaths(FullPath_, name);
    TFileOutput output(TFile(path, OpenMode::WrOnly));
    output << value;
#endif
}

std::vector<int> TNonOwningCGroup::GetTasks() const
{
    std::vector<int> results;
#ifdef _linux_
    auto values = ReadAllValues(NFS::CombinePaths(FullPath_, "tasks"));
    for (const auto& value : values) {
        int pid = FromString<int>(value);
        results.push_back(pid);
    }
#endif
    return results;
}

const Stroka& TNonOwningCGroup::GetFullPath() const
{
    return FullPath_;
}

void TNonOwningCGroup::EnsureExistance()
{
    LOG_INFO("Creating cgroup %s", ~FullPath_.Quote());

#ifdef _linux_
    NFS::ForcePath(FullPath_, 0755);
#endif
}

////////////////////////////////////////////////////////////////////////////////

TCGroup::TCGroup(const Stroka& type, const Stroka& name)
    : TNonOwningCGroup(type, name)
    , Created_(false)
{ }

TCGroup::TCGroup(TCGroup&& other)
    : TNonOwningCGroup(std::move(other))
    , Created_(other.Created_)
{
    other.Created_ = false;
}

TCGroup::~TCGroup()
{
    if (Created_) {
        try {
            Destroy();
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Unable to destroy cgroup %s", ~FullPath_.Quote());
        }
    }
}

void TCGroup::Create()
{
    EnsureExistance();
    Created_ = true;
}

void TCGroup::Destroy()
{
    LOG_INFO("Destroying cgroup %s", ~FullPath_.Quote());

#ifdef _linux_
    YCHECK(Created_);

    if (!NFS::Remove(FullPath_)) {
        THROW_ERROR(TError::FromSystem());
    }
    Created_ = false;
#endif
}

bool TCGroup::IsCreated() const
{
    return Created_;
}

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

TDuration FromJiffies(i64 jiffies)
{
    long ticksPerSecond = sysconf(_SC_CLK_TCK);
    return TDuration::MicroSeconds(1000 * 1000 * jiffies/ ticksPerSecond);
}

#endif

////////////////////////////////////////////////////////////////////////////////

TCpuAccounting::TStatistics::TStatistics()
    : UserTime(0)
    , SystemTime(0)
{ }

TCpuAccounting::TCpuAccounting(const Stroka& name)
    : TCGroup("cpuacct", name)
{ }

TCpuAccounting::TStatistics TCpuAccounting::GetStatistics()
{
    TCpuAccounting::TStatistics result;
#ifdef _linux_
    const auto path = NFS::CombinePaths(GetFullPath(), "cpuacct.stat");
    auto values = ReadAllValues(path);
    if (values.size() != 4) {
        THROW_ERROR_EXCEPTION("Unable to parse %s: expected 4 values, got %d", ~path.Quote(), values.size());
    }

    Stroka type[2];
    i64 jiffies[2];

    for (int i = 0; i < 2; ++i) {
        type[i] = values[2 * i];
        jiffies[i] = FromString<i64>(values[2 * i + 1]);
    }

    for (int i = 0; i < 2; ++i) {
        if (type[i] == "user") {
            result.UserTime = FromJiffies(jiffies[i]);
        } else if (type[i] == "system") {
            result.SystemTime = FromJiffies(jiffies[i]);
        }
    }
#endif
    return result;
}

void ToProto(NProto::TCpuAccountingStatistics* protoStats, const TCpuAccounting::TStatistics& stats)
{
    protoStats->set_user_time(stats.UserTime.MilliSeconds());
    protoStats->set_system_time(stats.SystemTime.MilliSeconds());
}

////////////////////////////////////////////////////////////////////////////////

TBlockIO::TStatistics::TStatistics()
    : TotalSectors(0)
    , BytesRead(0)
    , BytesWritten(0)
{ }

TBlockIO::TBlockIO(const Stroka& name)
    : TCGroup("blkio", name)
{ }

TBlockIO::TStatistics TBlockIO::GetStatistics()
{
    TBlockIO::TStatistics result;
#ifdef _linux_
    {
        const auto path = NFS::CombinePaths(GetFullPath(), "blkio.io_service_bytes");
        auto values = ReadAllValues(path);

        result.BytesRead = result.BytesWritten = 0;
        int lineNumber = 0;
        while (3 * lineNumber + 2 < values.size()) {
            const Stroka& deviceId = values[3 * lineNumber];
            const Stroka& type = values[3 * lineNumber + 1];
            i64 bytes = FromString<i64>(values[3 * lineNumber + 2]);

            if (!deviceId.has_prefix("8:")) {
                THROW_ERROR_EXCEPTION("Unable to parse %s: %s should start from 8:", ~path.Quote(), ~deviceId);
            }

            if (type == "Read") {
                result.BytesRead += bytes;
            } else if (type == "Write") {
                result.BytesWritten += bytes;
            } else {
                if (type != "Sync" && type != "Async" && type != "Total") {
                    THROW_ERROR_EXCEPTION("Unable to parse %s: unexpected stat type %s", ~path.Quote(), ~type);
                }
            }
            ++lineNumber;
        }
    }
    {
        const auto path = NFS::CombinePaths(GetFullPath(), "blkio.sectors");
        auto values = ReadAllValues(path);

        result.TotalSectors = 0;
        int lineNumber = 0;
        while (2 * lineNumber < values.size()) {
            const Stroka& deviceId = values[2 * lineNumber];
            i64 sectors = FromString<i64>(values[2 * lineNumber + 1]);

            if (!deviceId.has_prefix("8:")) {
                THROW_ERROR_EXCEPTION("Unable to parse %s: %s should start from 8:", ~path.Quote(), ~deviceId);
            }

            result.TotalSectors += sectors;
            ++lineNumber;
        }
    }
#endif
    return result;
}

void ToProto(NProto::TBlockIOStatistics* protoStats, const TBlockIO::TStatistics& stats)
{
    protoStats->set_total_sectors(stats.TotalSectors);
    protoStats->set_bytes_read(stats.BytesRead);
    protoStats->set_bytes_written(stats.BytesWritten);
}

////////////////////////////////////////////////////////////////////////////////

TMemory::TMemory(const Stroka& name)
    : TCGroup("memory", name)
{ }

TMemory::TMemory(TMemory&& other)
    : TCGroup(std::move(other))
{ }


TMemory::TStatistics TMemory::GetStatistics()
{
    TMemory::TStatistics result;
#ifdef _linux_
    {
        const auto filename = NFS::CombinePaths(GetFullPath(), "memory.usage_in_bytes");
        auto rawData = TFileInput(filename).ReadAll();
        result.UsageInBytes = FromString<i64>(strip(rawData));
    }

    {
        const auto filename = NFS::CombinePaths(GetFullPath(), "memory.max_usage_in_bytes");
        auto rawData = TFileInput(filename).ReadAll();
        result.MaxUsageInBytes = FromString<i64>(strip(rawData));
    }

    {
        auto values = ReadAllValues(NFS::CombinePaths(FullPath_, "memory.stat"));
        int lineNumber = 0;
        while (2 * lineNumber + 1 < values.size()) {
            const Stroka& type = values[2 * lineNumber];
            const i64 value = FromString<i64>(values[2 * lineNumber + 1]);
            if (type == "rss") {
                result.Rss = value;
            }
            ++lineNumber;
        }
    }
#endif
    return result;
}

void TMemory::SetLimitInBytes(i64 bytes) const
{
    Set("memory.limit_in_bytes", ToString(bytes));
}

bool TMemory::IsHierarchyEnabled() const
{
#ifdef _linux_
    const auto filename = NFS::CombinePaths(GetFullPath(), "memory.use_hierarchy");
    auto isHierarchyEnabled = FromString<int>(TFileInput(filename).ReadAll());
    YCHECK((isHierarchyEnabled == 0) || (isHierarchyEnabled == 1));

    return (isHierarchyEnabled == 1);
#else
    return false;
#endif
}

void TMemory::EnableHierarchy() const
{
    Set("memory.use_hierarchy", "1");
}

bool TMemory::IsOomEnabled() const
{
#ifdef _linux_
    const auto path = NFS::CombinePaths(GetFullPath(), "memory.oom_control");
    auto values = ReadAllValues(path);
    if (values.size() != 4) {
        THROW_ERROR_EXCEPTION("Unable to parse %s: expected 4 values, got %d", ~path.Quote(), values.size());
    }
    for (int i = 0; i < 2; ++i) {
        if (values[2 * i] == "oom_kill_disable") {
            const auto& isDisabled = values[2 * i + 1];
            if (isDisabled == "0") {
                return true;
            } else if (isDisabled == "1") {
                return false;
            } else {
                THROW_ERROR_EXCEPTION("Unexpected value for oom_kill_disable. Expected '0' or '1'. Got: %s",
                    ~isDisabled.Quote());
            }
        }
    }
    THROW_ERROR_EXCEPTION("Unable to find 'oom_kill_disable' in %s", ~path.Quote());
#else
    return false;
#endif
}

void TMemory::DisableOom() const
{
    // This parameter should be call `memory.disable_oom_control`.
    // 1 means `disable`.
    Set("memory.oom_control", "1");
}

TEvent TMemory::GetOomEvent() const
{
#ifdef _linux_
    const auto filename = NFS::CombinePaths(GetFullPath(), "memory.oom_control");
    auto fd = ::open(~filename, O_WRONLY | O_CLOEXEC);

    auto eventFd = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    auto data = ToString(eventFd) + ' ' + ToString(fd);

    Set("cgroup.event_control", data);

    return TEvent(eventFd, fd);
#else
    return TEvent();
#endif
}

int TMemory::GetFailCount() const
{
    int failCount = 0;
#ifdef _linux_
    const auto filename = NFS::CombinePaths(GetFullPath(), "memory.failcnt");
    failCount = FromString<int>(Strip(TFileInput(filename).ReadAll()));
#endif
    return failCount;
}

TMemory::TStatistics::TStatistics()
    : UsageInBytes(0)
    , MaxUsageInBytes(0)
{ }

////////////////////////////////////////////////////////////////////////////////

std::map<Stroka, Stroka> ParseCurrentProcessCGroups(TStringBuf str)
{
    std::map<Stroka, Stroka> result;

    yvector<Stroka> values;
    Split(str.data(), ":\n", values);
    for (size_t i = 0; i + 2 < values.size(); i += 3) {
        FromString<int>(values[i]);

        const Stroka& subsystemsSet = values[i + 1];
        const Stroka& name = values[i + 2];

        yvector<Stroka> subsystems;
        Split(subsystemsSet.data(), ",", subsystems);
        for (const auto& subsystem : subsystems) {
            if (!subsystem.has_prefix("name=")) {
                int start = 0;
                if (name.has_prefix("/")) {
                    start = 1;
                }
                result[subsystem] = name.substr(start);
            }
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCGroup
} // namespace NYT
