#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/stracer.h>

#include <util/system/thread.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TStracer, EmptyPidsList)
{
    auto result = Strace(std::vector<int>());
    EXPECT_TRUE(result->Traces.empty());
}

#ifdef _linux_
TEST(TStracer, Basic)
{
    if (setuid(0) != 0) {
        return;
    }

    int pid = fork();
    ASSERT_TRUE(pid >= 0);

    if (pid == 0) {
        TThread::CurrentThreadSetName("SomeCoolProcess");
        while (true) {
            ssize_t ignored __attribute__((unused));
            ignored = write(42, "hello\n", 6);
        }
        exit(0);
    }

    sleep(1);

    auto result = Strace(std::vector<int>({pid}));

    ASSERT_EQ(0, kill(pid, SIGKILL));
    ASSERT_EQ(pid, waitpid(pid, nullptr, 0));

    EXPECT_TRUE(result->Traces[pid]->ProcessName == "SomeCoolProcess")
        << result->Traces[pid]->ProcessName;
    EXPECT_TRUE(result->Traces[pid]->Trace.find("write(42, \"hello\\n\", 6) = -1 EBADF") != TString::npos)
        << result->Traces[pid]->Trace;
}
#endif

////////////////////////////////////////////////////////////////////////////////
} // namespace
} // namespace NYT
