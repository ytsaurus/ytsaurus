#include "stdafx.h"
#include "private.h"
#include "cgroup.h"

#include <core/misc/fs.h>
#include <core/misc/error.h>

#include <util/folder/dirut.h>
#include <util/system/fs.h>
#include <util/string/split.h>

#include <fstream>
#include <sstream>

namespace NYT {
namespace NCGroup {

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = CGroupLogger;
static const char* CGroupRootPath = "/sys/fs/cgroup";

////////////////////////////////////////////////////////////////////////////////

Stroka GetParentFor(const Stroka& type);

TCGroup::TCGroup(const Stroka& type, const Stroka& name)
    : FullPath_(NFS::CombinePaths(NFS::CombinePaths(NFS::CombinePaths(CGroupRootPath,  type), GetParentFor(type)), name))
    , Created_(false)
{ }

TCGroup::~TCGroup()
{
    if (Created_) {
        try {
            Destroy();
        } catch (const TErrorException& ) {
            LOG_ERROR("Unable to destroy cgroup %s", ~FullPath_);
        }
    }
}

void TCGroup::Create()
{
    LOG_INFO("Create cgroup %s", ~FullPath_);

#ifdef _linux_
    int hasError = Mkdir(FullPath_.data(), 0755);
    if (hasError != 0) {
        THROW_ERROR_EXCEPTION("Unable to create cgroup %s", ~FullPath_) << TError::FromSystem();
    }
    Created_ = true;
#endif
}

void TCGroup::Destroy()
{
    LOG_INFO("Destroy cgroup %s", ~FullPath_);

#ifdef _linux_
    YCHECK(Created_);

    int hasError = NFs::Remove(FullPath_.data());
    if (hasError != 0) {
        THROW_ERROR(TError::FromSystem());
    }
    Created_ = false;
#endif
}

void TCGroup::AddCurrentProcess()
{
#ifdef _linux_
    auto pid = getpid();
    LOG_INFO("Add process %d to cgroup %s", pid, ~FullPath_);

    std::ofstream tasks(NFS::CombinePaths(FullPath_, "tasks").data(), std::ios_base::app);
    tasks << getpid() << std::endl;
#endif
}

std::vector<int> TCGroup::GetTasks()
{
    std::vector<int> results;
#ifdef _linux_
    std::ifstream tasks(NFS::CombinePaths(FullPath_, "tasks").data());
    if (tasks.fail()) {
        THROW_ERROR_EXCEPTION("Unable to open a task list file");
    }

    while (!tasks.eof()) {
        int pid;
        tasks >> pid;
        if (tasks.bad()) {
            THROW_ERROR_EXCEPTION("Unable to read a task list");
        }
        if (tasks.good()) {
            results.push_back(pid);
        }
    }
#endif
    return results;
}

const Stroka& TCGroup::GetFullPath() const
{
    return FullPath_;
}

bool TCGroup::IsCreated() const
{
    return Created_;
}

////////////////////////////////////////////////////////////////////////////////

std::vector<char> ReadAll(const Stroka& fileName)
{
    const size_t blockSize = 4096;
    std::vector<char> buffer(blockSize, 0);
    size_t alreadyRead = 0;

    std::fstream file(fileName.data(), std::ios_base::in);
    if (file.fail()) {
        // add a name of file
        THROW_ERROR_EXCEPTION("Unable to open a file");
    }

    while (file.good()) {
        file.read(buffer.data() + alreadyRead, buffer.size() - alreadyRead);
        alreadyRead = buffer.size();

        buffer.resize(buffer.size() + blockSize);
    }
    if (file.bad()) {
        // add a name of file
        THROW_ERROR_EXCEPTION("Unable to read data from a file");
    }

    if (file.eof()) {
        alreadyRead += file.gcount();

        buffer.resize(alreadyRead + 1);
        buffer[alreadyRead] = 0;
    }

    return buffer;
}

#ifdef _linux_

std::chrono::nanoseconds fromJiffies(int64_t jiffies)
{
    long ticksPerSecond = sysconf(_SC_CLK_TCK);
    return std::chrono::nanoseconds(1000 * 1000 * 1000 * jiffies/ ticksPerSecond);
}

#endif

////////////////////////////////////////////////////////////////////////////////

TCpuAccounting::TCpuAccounting(const Stroka& name)
    : TCGroup("cpuacct", name)
{ }

TCpuAccounting::TStats TCpuAccounting::GetStats()
{
    TCpuAccounting::TStats result;
#ifdef _linux_
    const Stroka filename = NFS::CombinePaths(GetFullPath(), "cpuacct.stat");
    std::vector<char> statsRaw = ReadAll(filename);
    yvector<Stroka> values;
    int count = Split(statsRaw.data(), " \n", values);
    if (count != 4) {
        THROW_ERROR_EXCEPTION("Unable to parse %s. Expected 4 values. Got %d", ~filename, count);
    }

    std::string type[2];
    int64_t jiffies[2];

    for (int i = 0; i < 2; ++i) {
        type[i] = values[2 * i];
        jiffies[i] = FromString<int64_t>(~values[2 * i + 1]);
    }

    for (int i = 0; i < 2; ++ i) {
        if (type[i] == "user") {
            result.User = fromJiffies(jiffies[i]);
        } else if (type[i] == "system") {
            result.System = fromJiffies(jiffies[i]);
        }
    }
#endif
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TBlockIO::TBlockIO(const Stroka& name)
    : TCGroup("blkio", name)
{ }

TBlockIO::TStats TBlockIO::GetStats()
{
    TBlockIO::TStats result;
#ifdef _linux_
    {
        const Stroka filename = NFS::CombinePaths(GetFullPath(), "blkio.io_service_bytes");
        std::vector<char> statsRaw = ReadAll(filename);
        yvector<Stroka> values;
        Split(statsRaw.data(), " \n", values);

        result.BytesRead = result.BytesWritten = 0;
        int line_number = 0;
        while (3 * line_number + 2 < values.size()) {
            const Stroka& deviceId = values[3 * line_number];
            const Stroka& type = values[3 * line_number + 1];
            int64_t bytes = FromString<int64_t>(~values[3 * line_number + 2]);

            if ((deviceId.Size() <= 2) || (deviceId[0] != '8') || (deviceId[1] != ':')) {
                THROW_ERROR_EXCEPTION("Unable to parse %s. %s should start from 8:", ~filename, ~deviceId);
            }

            if (type == "Read") {
                result.BytesRead += bytes;
            } else if (type == "Write") {
                result.BytesWritten += bytes;
            } else {
                if ((type != "Sync") && (type != "Async") && (type != "Total")) {
                    THROW_ERROR_EXCEPTION("Unable to parse %s. Unexpected stat type: %s", ~filename, ~type);
                }
            }
            ++line_number;
        }
    }
    {
        const Stroka filename = NFS::CombinePaths(GetFullPath(), "blkio.sectors");
        std::vector<char> statsRaw = ReadAll(filename);
        yvector<Stroka> values;
        Split(statsRaw.data(), " \n", values);

        result.Sectors = 0;
        int line_number = 0;
        while (2 * line_number < values.size()) {
            const Stroka& deviceId = values[2 * line_number];
            int64_t sectors = FromString<int64_t>(~values[2 * line_number + 1]);

            if ((deviceId.Size() <= 2) || (deviceId[0] != '8') || (deviceId[1] != ':')) {
                THROW_ERROR_EXCEPTION("Unable to parse %s. %s should start from 8:", ~filename, ~deviceId);
            }

            result.Sectors += sectors;
            ++line_number;
        }
    }
#endif
    return result;
}

////////////////////////////////////////////////////////////////////////////////

std::map<Stroka, Stroka> ParseCurrentProcessCGrops(const char* str, size_t size)
{
    std::map<Stroka, Stroka> result;

    yvector<Stroka> values;
    Split(str, ":\n", values);
    for (size_t i = 0; i + 2 < values.size(); i += 3) {
        FromString<int>(values[i]);

        const Stroka& setOfSubsystems = values[i + 1];
        const Stroka& name = values[i + 2];

        yvector<Stroka> subsystems;
        Split(setOfSubsystems.data(), ",", subsystems);
        for (size_t j = 0; j < subsystems.size(); ++j) {
            int start = 0;
            if ((!name.empty()) && (name[0] == '/')) {
                start = 1;
            }
            result[subsystems[j]] = name.substr(start);
        }
    }

    return result;
}

Stroka GetParentFor(const Stroka& type)
{
    auto rawData = ReadAll("/proc/self/cgroup");
    auto result = ParseCurrentProcessCGrops(rawData.data(), rawData.size());
    return result[type];
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCGroup
} // namespace NYT
