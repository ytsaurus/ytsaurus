#include "stdafx.h"
#include "cgroup.h"

#include <core/misc/fs.h>
#include <core/misc/error.h>

#include <util/folder/dirut.h>
#include <util/system/fs.h>

#include <fstream>

namespace NYT {
namespace NCGroup {

////////////////////////////////////////////////////////////////////////////////

TCGroup::TCGroup(const Stroka& parent, const Stroka& name)
    : FullName_(NFS::CombinePaths(parent, name))
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
#ifndef _win_
    int hasError = Mkdir(FullName_.data(), 0755);
    if (hasError != 0) {
        THROW_ERROR(TError::FromSystem());
    }
    Created_ = true;
#endif
}

void TCGroup::Destroy()
{
#ifndef _win_
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
#ifndef _win_
    std::fstream tasks(NFS::CombinePaths(FullName_, "tasks").data(), std::ios_base::out | std::ios_base::app);
    tasks << getpid() << std::endl;
#endif
}

std::vector<int> TCGroup::GetTasks()
{
    std::vector<int> results;
#ifndef _win_
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

////////////////////////////////////////////////////////////////////////////////

#ifndef _win_

std::chrono::nanoseconds from_jiffs(int64_t jiffs)
{
    long ticks_per_second = sysconf(_SC_CLK_TCK);
    return std::chrono::nanoseconds(1000 * 1000 * 1000 * jiffs/ ticks_per_second);
}

#endif

TCpuAcctStat GetCpuAccStat(const Stroka& fullName)
{
    TCpuAcctStat result;
#ifndef _win_
    std::fstream stats(NFS::CombinePaths(fullName, "cpuacct.stat").data(), std::ios_base::in);
    if (stats.fail()) {
        THROW_ERROR_EXCEPTION("Unable to open a cpuacc stat");
    }

    std::string type[2];
    int64_t jiffs[2];

    stats >> type[0] >> jiffs[0] >> type[1] >> jiffs[1];
    if (stats.fail()) {
        THROW_ERROR_EXCEPTION("Unable to read cpu stats");
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

} // namespace NCGroup
} // namespace NYT
