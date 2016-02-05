#include "framework.h"

#include <yt/core/actions/bind.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/process.h>
#include <yt/core/misc/proc.h>

namespace NYT {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

#ifdef _unix_

TEST(TProcessTest, Basic)
{
    auto p = New<TProcess>("/bin/ls");
    TFuture<void> finished;

    ASSERT_NO_THROW(finished = p->Spawn());
    ASSERT_TRUE(p->IsStarted());
    auto error = WaitFor(finished);
    EXPECT_TRUE(error.IsOK()) << ToString(error);
    EXPECT_TRUE(p->IsFinished());
}

TEST(TProcessTest, InvalidPath)
{
    auto p = New<TProcess>("/some/bad/path/binary");

    TFuture<void> finished;
    ASSERT_NO_THROW(finished = p->Spawn());
    ASSERT_TRUE(p->IsStarted());
    auto error = WaitFor(finished);
    EXPECT_TRUE(p->IsFinished());
    EXPECT_FALSE(error.IsOK());
    EXPECT_EQ("Error starting child process: execve failed", error.GetMessage());
}

TEST(TProcessTest, BadDup)
{
    auto p = New<TProcess>("/bin/date");
    p->AddDup2FileAction(1000, 1);

    TFuture<void> finished;
    ASSERT_NO_THROW(finished = p->Spawn());
    ASSERT_TRUE(p->IsStarted());
    auto error = WaitFor(finished);
    EXPECT_TRUE(p->IsFinished());
    EXPECT_FALSE(error.IsOK());
    EXPECT_EQ("Error duplicating 1000 file descriptor to 1 in child process", error.GetMessage());
}

TEST(TProcessTest, GoodDup)
{
    auto p = New<TProcess>("/bin/date");
    p->AddDup2FileAction(2, 3);

    TFuture<void> finished;
    ASSERT_NO_THROW(finished = p->Spawn());
    ASSERT_TRUE(p->IsStarted());
    auto error = WaitFor(finished);
    EXPECT_TRUE(error.IsOK()) << ToString(error);
    EXPECT_TRUE(p->IsFinished());
}

TEST(TProcess, GetCommandLine)
{
    auto p = New<TProcess>("/bin/bash");
    EXPECT_EQ("/bin/bash", p->GetCommandLine());
    p->AddArgument("-c");
    EXPECT_EQ("/bin/bash -c", p->GetCommandLine());
    p->AddArgument("exit 0");
    EXPECT_EQ("/bin/bash -c \"exit 0\"", p->GetCommandLine());
}

TEST(TProcess, IgnoreCloseInvalidFD)
{
    auto p = New<TProcess>("/bin/bash");
    p->AddArgument("-c");
    p->AddArgument("exit 0");
    p->AddCloseFileAction(74);

    TFuture<void> finished;
    ASSERT_NO_THROW(finished = p->Spawn());
    ASSERT_TRUE(p->IsStarted());
    auto error = WaitFor(finished);
    EXPECT_TRUE(error.IsOK()) << ToString(error);
    EXPECT_TRUE(p->IsFinished());
}

TEST(TProcessTest, ProcessReturnCode0)
{
    auto p = New<TProcess>("/bin/bash");
    p->AddArgument("-c");
    p->AddArgument("exit 0");

    TFuture<void> finished;
    ASSERT_NO_THROW(finished = p->Spawn());
    ASSERT_TRUE(p->IsStarted());
    auto error = WaitFor(finished);
    EXPECT_TRUE(error.IsOK()) << ToString(error);
    EXPECT_TRUE(p->IsFinished());
}

TEST(TProcessTest, ProcessReturnCode123)
{
    auto p = New<TProcess>("/bin/bash");
    p->AddArgument("-c");
    p->AddArgument("exit 123");

    TFuture<void> finished;
    ASSERT_NO_THROW(finished = p->Spawn());
    ASSERT_TRUE(p->IsStarted());
    auto error = WaitFor(finished);
    EXPECT_FALSE(error.IsOK());
    EXPECT_EQ("Process exited with code 123", error.GetMessage());
    EXPECT_EQ(TErrorCode(static_cast<int>(EExitStatus::SignalBase) + 123), error.GetCode());
    EXPECT_TRUE(p->IsFinished());
}

TEST(TProcessTest, Params1)
{
    auto p = New<TProcess>("/bin/bash");
    p->AddArgument("-c");
    p->AddArgument("if test 3 -gt 1; then exit 7; fi");

    auto error = WaitFor(p->Spawn());
    EXPECT_FALSE(error.IsOK());
    EXPECT_TRUE(p->IsFinished());
}

TEST(TProcessTest, Params2)
{
    auto p = New<TProcess>("/bin/bash");
    p->AddArgument("-c");
    p->AddArgument("if test 1 -gt 3; then exit 7; fi");

    auto error = WaitFor(p->Spawn());
    EXPECT_TRUE(error.IsOK()) << ToString(error);
    EXPECT_TRUE(p->IsFinished());
}

TEST(TProcessTest, InheritEnvironment)
{
    const char* name = "SPAWN_TEST_ENV_VAR";
    const char* value = "42";
    setenv(name, value, 1);

    auto p = New<TProcess>("/bin/bash");
    p->AddArgument("-c");
    p->AddArgument("if test $SPAWN_TEST_ENV_VAR = 42; then exit 7; fi");

    auto error = WaitFor(p->Spawn());
    EXPECT_FALSE(error.IsOK());
    EXPECT_TRUE(p->IsFinished());

    unsetenv(name);
}

TEST(TProcessTest, Kill)
{
    auto p = New<TProcess>("/bin/sleep");
    p->AddArgument("1");

    auto finished = p->Spawn();

    NConcurrency::TDelayedExecutor::Submit(
        BIND([&] () {
            p->Kill(SIGKILL);
        }),
        TDuration::MilliSeconds(100));

    auto error = WaitFor(finished);
    EXPECT_FALSE(error.IsOK());
    EXPECT_TRUE(p->IsFinished());
}

TEST(TProcessTest, KillFinished)
{
    auto p = New<TProcess>("/bin/bash");
    p->AddArgument("-c");
    p->AddArgument("true");

    auto finished = p->Spawn();

    auto error = WaitFor(finished);
    EXPECT_TRUE(error.IsOK());

    p->Kill(SIGKILL);
}

TEST(TProcessTest, KillZombie)
{
    auto p = New<TProcess>("/bin/bash");
    p->AddArgument("-c");
    p->AddArgument("true");

    auto finished = p->Spawn();

    siginfo_t infop;
    auto res = ::waitid(P_PID, p->GetProcessId(), &infop, WEXITED | WNOWAIT);
    EXPECT_TRUE(res == 0);
    EXPECT_EQ(p->GetProcessId(), infop.si_pid);

    p->Kill(SIGKILL);
    auto error = WaitFor(finished);
    EXPECT_TRUE(error.IsOK());
}

TEST(TProcessTest, PollDuration)
{
    auto p = New<TProcess>("/bin/sleep", true, TDuration::MilliSeconds(1));
    p->AddArgument("0.1");

    auto error = WaitFor(p->Spawn());
    EXPECT_TRUE(error.IsOK()) << ToString(error);
    EXPECT_TRUE(p->IsFinished());
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
