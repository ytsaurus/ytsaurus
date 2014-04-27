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

////////////////////////////////////////////////////////////////////////////////

TCGroup::TCGroup(const Stroka& parent, const Stroka& name)
    : FullName_(NFS::CombinePaths(parent, name))
    , Created_(false)
{ }

TCGroup::~TCGroup()
{
    if (Created_) {
        try {
            Destroy();
        } catch (const TErrorException& ) {
            LOG_ERROR("Unable to destroy a cgroup: %s", ~FullName_);
        }
    }
}

void TCGroup::Create()
{
    LOG_INFO("Create cgroup: %s", ~FullName_);

#ifdef _linux_
    int hasError = Mkdir(FullName_.data(), 0755);
    if (hasError != 0) {
        THROW_ERROR(TError::FromSystem());
    }
    Created_ = true;
#endif
}

void TCGroup::Destroy()
{
    LOG_INFO("Destroy cgroup: %s", ~FullName_);

#ifdef _linux_
    YCHECK(Created_);

    int hasError = NFs::Remove(FullName_.data());
    if (hasError != 0) {
        THROW_ERROR(TError::FromSystem());
    }
    Created_ = false;
#endif
}

void TCGroup::AddMyself()
{
#ifdef _linux_
    auto pid = getpid();
    LOG_INFO("Add process %d to cgroup: %s", pid, ~FullName_);

    std::fstream tasks(NFS::CombinePaths(FullName_, "tasks").data(), std::ios_base::out | std::ios_base::app);
    tasks << getpid() << std::endl;
#endif
}

std::vector<int> TCGroup::GetTasks()
{
    std::vector<int> results;
#ifdef _linux_
    std::fstream tasks(NFS::CombinePaths(FullName_, "tasks").data(), std::ios_base::in);
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

const Stroka& TCGroup::GetFullName() const
{
    return FullName_;
}

bool TCGroup::IsCreated() const
{
    return Created_;
}

////////////////////////////////////////////////////////////////////////////////

template<typename T>
T To(const char* str)
{
    T result;
    std::stringstream stream;
    stream << str;
    stream >> result;
    return result;
}

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

std::chrono::nanoseconds from_jiffs(int64_t jiffs)
{
    long ticks_per_second = sysconf(_SC_CLK_TCK);
    return std::chrono::nanoseconds(1000 * 1000 * 1000 * jiffs/ ticks_per_second);
}

#endif

TCpuAcctStat GetCpuAccStat(const Stroka& fullName)
{
    TCpuAcctStat result;
#ifdef _linux_
    std::vector<char> statsRaw = ReadAll(NFS::CombinePaths(fullName, "cpuacct.stat"));
    yvector<Stroka> values;
    int count = Split(statsRaw.data(), " \n", values);
    YCHECK(count == 4);

    std::string type[2];
    int64_t jiffs[2];

    for (int i = 0; i < 2; ++i) {
        type[i] = values[2 * i];
        jiffs[i] = To<int64_t>(~values[2 * i + 1]);
    }

    for (int i = 0; i < 2; ++ i) {
        if (type[i] == "user") {
            result.user = from_jiffs(jiffs[i]);
        } else if (type[i] == "system") {
            result.system = from_jiffs(jiffs[i]);
        }
    }
#endif
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TBlockIOStat GetBlockIOStat(const Stroka& fullName)
{
    TBlockIOStat result;
#ifdef _linux_
    {
        std::vector<char> statsRaw = ReadAll(NFS::CombinePaths(fullName, "blkio.io_service_bytes").data());
        yvector<Stroka> values;
        Split(statsRaw.data(), " \n", values);

        result.ReadBytes = result.WriteBytes = 0;
        int line_number = 0;
        while (3 * line_number + 2 < values.size()) {
            const Stroka& deviceId = values[3 * line_number];
            const Stroka& type = values[3 * line_number + 1];
            int64_t bytes = To<int64_t>(~values[3 * line_number + 2]);

            YCHECK(deviceId.Size() > 2);
            YCHECK(deviceId[0] == '8');
            YCHECK(deviceId[1] == ':');

            if (type == "Read") {
                result.ReadBytes += bytes;
            } else if (type == "Write") {
                result.WriteBytes += bytes;
            } else {
                YCHECK((type == "Sync") || (type == "Async") || (type == "Total"));
            }
            ++line_number;
        }
    }
    {
        std::vector<char> statsRaw = ReadAll(NFS::CombinePaths(fullName, "blkio.sectors").data());
        yvector<Stroka> values;
        Split(statsRaw.data(), " \n", values);

        result.Sectors = 0;
        int line_number = 0;
        while (2 * line_number < values.size()) {
            const Stroka& deviceId = values[2 * line_number];
            int64_t sectors = To<int64_t>(~values[2 * line_number + 1]);

            YCHECK(deviceId.Size() > 2);
            YCHECK(deviceId[0] == '8');
            YCHECK(deviceId[1] == ':');

            result.Sectors += sectors;
            ++line_number;
        }
    }
#endif
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCGroup
} // namespace NYT
