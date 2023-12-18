#include "rusage.h"

#include <sys/time.h>
#include <sys/resource.h>
#include <unistd.h>

namespace NYT::NIOTest {

////////////////////////////////////////////////////////////////////////////////

TRusage TRusage::operator+=(const TRusage& other)
{
    UserTime += other.UserTime;
    SystemTime += other.SystemTime;
    return *this;
}

TRusage operator+(const TRusage& lhs, const TRusage& rhs)
{
    return {lhs.UserTime + rhs.UserTime, lhs.SystemTime + rhs.SystemTime};
}

TRusage operator-(const TRusage& lhs, const TRusage& rhs)
{
    return {lhs.UserTime - rhs.UserTime, lhs.SystemTime - rhs.SystemTime};
}

////////////////////////////////////////////////////////////////////////////////

TRusage GetRusage(ERusageWho who)
{
    struct rusage usage;
    int r = getrusage(who == ERusageWho::Process ? RUSAGE_SELF : RUSAGE_THREAD, &usage);
    if (r < 0) {
        ythrow TSystemError() << "rusage failed";
    }
    return {usage.ru_utime, usage.ru_stime};
}

////////////////////////////////////////////////////////////////////////////////

TRusage GetProcessRusage(TProcessId processId)
{
    auto pstat = ReadProcessStat(processId);
    if (pstat.empty()) {
        return {};
    }

    auto ticks = sysconf(_SC_CLK_TCK);
    auto utime = FromString<i64>(pstat[ToUnderlying(EProcessStatField::UserTime)]) * 1000 / ticks;
    auto stime = FromString<i64>(pstat[ToUnderlying(EProcessStatField::SystemTime)]) * 1000 / ticks;
    return {TDuration::MilliSeconds(utime), TDuration::MilliSeconds(stime)};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest

