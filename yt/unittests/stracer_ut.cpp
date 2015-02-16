#include "stdafx.h"
#include "framework.h"

#include <server/job_proxy/stracer.h>

#include <core/concurrency/thread.h>

namespace NYT {
namespace NJobProxy {
namespace {

////////////////////////////////////////////////////////////////////

TEST(TStracer, EmptyPidsList)
{
    auto result = Strace(std::vector<int>());
    EXPECT_TRUE(result.Traces.empty());
}

TEST(TStracer, Basic)
{
    if (setuid(0) != 0) {
        return;
    }

    int pid = fork();
    ASSERT_TRUE(pid >= 0);

    if (pid == 0) {
        NConcurrency::SetCurrentThreadName("SomeCoolProcess");
        int secondToRun = 20;
        while (secondToRun > 0) {
            write(42, "hello\n", 6);
            sleep(1);
            --secondToRun;
        }
        exit(0);
    }

    sleep(1);

    auto result = Strace(std::vector<int>({pid}));

    ASSERT_EQ(0, kill(pid, 9));
    ASSERT_EQ(pid, waitpid(pid, NULL, 0));

    EXPECT_TRUE(result.Traces[pid].ProcessName == "SomeCoolProcess")
        << result.Traces[pid].ProcessName;
    EXPECT_TRUE(result.Traces[pid].Trace.find("write(42, \"hello\\n\", 6) = -1 EBADF") != Stroka::npos)
        << result.Traces[pid].Trace;
}

////////////////////////////////////////////////////////////////////
} // namespace
} // namespace NJobProxy
} // namespace NYT