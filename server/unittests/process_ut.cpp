#include <yt/core/test_framework/framework.h>

#ifdef _linux_

#include <yt/core/actions/bind.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/guid.h>
#include <yt/core/misc/proc.h>

#include <yt/core/net/connection.h>

#include <yt/server/misc/process.h>

#include <yt/server/containers/porto_executor.h>
#include <yt/server/containers/instance.h>

#include <util/system/platform.h>

namespace NYT {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static TString GetUniqueName()
{
    return "yt_ut_" + ToString(TGuid::Create());
}

TEST(TPortoProcessTest, Basic)
{
    auto portoInstance = NContainers::CreatePortoInstance(
        GetUniqueName(),
        NContainers::CreatePortoExecutor());
    auto p = New<TPortoProcess>("/bin/ls", portoInstance, true);
    TFuture<void> finished;
    ASSERT_NO_THROW(finished = p->Spawn());
    ASSERT_TRUE(p->IsStarted());
    auto error = WaitFor(finished);
    EXPECT_TRUE(error.IsOK()) << ToString(error);
    EXPECT_TRUE(p->IsFinished());
    portoInstance->Destroy();
}

TEST(TPortoProcessTest, RunFromPathEnv)
{
    auto portoInstance = NContainers::CreatePortoInstance(
        GetUniqueName(),
        NContainers::CreatePortoExecutor());
    auto p = New<TPortoProcess>("ls", portoInstance, true);
    TFuture<void> finished;
    ASSERT_NO_THROW(finished = p->Spawn());
    ASSERT_TRUE(p->IsStarted());
    auto error = WaitFor(finished);
    EXPECT_TRUE(error.IsOK()) << ToString(error);
    EXPECT_TRUE(p->IsFinished());
    portoInstance->Destroy();
}

TEST(TPortoProcessTest, MultiBasic)
{
    auto portoExecutor = NContainers::CreatePortoExecutor();
    auto c1 = NContainers::CreatePortoInstance(GetUniqueName(), portoExecutor);
    auto c2 = NContainers::CreatePortoInstance(GetUniqueName(), portoExecutor);
    auto p1 = New<TPortoProcess>("/bin/ls", c1, true);
    auto p2 = New<TPortoProcess>("/bin/ls", c2, true);
    TFuture<void> f1;
    TFuture<void> f2;
    ASSERT_NO_THROW(f1 = p1->Spawn());
    ASSERT_NO_THROW(f2 = p2->Spawn());
    auto error = WaitFor((Combine(std::vector<TFuture<void>>{f1, f2})));
    EXPECT_TRUE(error.IsOK()) << ToString(error);
    EXPECT_TRUE(p1->IsFinished());
    EXPECT_TRUE(p2->IsFinished());
    c1->Destroy();
    c2->Destroy();
}

TEST(TPortoProcessTest, InvalidPath)
{
    auto portoInstance = NContainers::CreatePortoInstance(
        GetUniqueName(),
        NContainers::CreatePortoExecutor());
    auto p = New<TPortoProcess>("/some/bad/path/binary", portoInstance, true);
    TFuture<void> finished;
    ASSERT_NO_THROW(finished = p->Spawn());
    ASSERT_TRUE(p->IsStarted());
    auto error = WaitFor(finished);
    EXPECT_TRUE(p->IsFinished());
    EXPECT_FALSE(error.IsOK());
    portoInstance->Destroy();
}

TEST(TPortoProcessTest, StdOut)
{
    auto portoInstance = NContainers::CreatePortoInstance(
        GetUniqueName(),
        NContainers::CreatePortoExecutor());
    auto p = New<TPortoProcess>("/bin/date", portoInstance, true);

    auto outStream = p->GetStdOutReader();
    TFuture<void> finished;
    ASSERT_NO_THROW(finished = p->Spawn());
    ASSERT_TRUE(p->IsStarted());
    auto error = WaitFor(finished);
    EXPECT_TRUE(error.IsOK()) << ToString(error);
    EXPECT_TRUE(p->IsFinished());

    auto buffer = TSharedMutableRef::Allocate(4096, false);
    auto future = outStream->Read(buffer);
    TErrorOr<size_t> result = WaitFor(future);
    size_t sz = result.ValueOrThrow();
    EXPECT_TRUE(sz > 0);
    portoInstance->Destroy();
}

TEST(TPortoProcessTest, GetCommandLine)
{
    auto portoInstance = NContainers::CreatePortoInstance(
        GetUniqueName(),
        NContainers::CreatePortoExecutor());
    auto p = New<TPortoProcess>("/bin/bash", portoInstance, true);
    EXPECT_EQ("/bin/bash", p->GetCommandLine());
    p->AddArgument("-c");
    EXPECT_EQ("/bin/bash -c", p->GetCommandLine());
    p->AddArgument("exit 0");
    EXPECT_EQ("/bin/bash -c \"exit 0\"", p->GetCommandLine());
    portoInstance->Destroy();
}

TEST(TPortoProcessTest, ProcessReturnCode0)
{
    auto portoInstance = NContainers::CreatePortoInstance(
        GetUniqueName(),
        NContainers::CreatePortoExecutor());
    auto p = New<TPortoProcess>("/bin/bash", portoInstance, true);
    p->AddArgument("-c");
    p->AddArgument("exit 0");

    TFuture<void> finished;
    ASSERT_NO_THROW(finished = p->Spawn());
    ASSERT_TRUE(p->IsStarted());
    auto error = WaitFor(finished);
    EXPECT_TRUE(error.IsOK()) << ToString(error);
    EXPECT_TRUE(p->IsFinished());
    portoInstance->Destroy();
}

TEST(TPortoProcessTest, ProcessReturnCode123)
{
    auto portoInstance = NContainers::CreatePortoInstance(
        GetUniqueName(),
        NContainers::CreatePortoExecutor());
    auto p = New<TPortoProcess>("/bin/bash", portoInstance, true);
    p->AddArgument("-c");
    p->AddArgument("exit 123");

    TFuture<void> finished;
    ASSERT_NO_THROW(finished = p->Spawn());
    ASSERT_TRUE(p->IsStarted());
    auto error = WaitFor(finished);
    EXPECT_EQ(EProcessErrorCode::NonZeroExitCode, error.GetCode());
    EXPECT_EQ(123, error.Attributes().Get<int>("exit_code"));
    EXPECT_TRUE(p->IsFinished());
    portoInstance->Destroy();
}

TEST(TPortoProcessTest, Params1)
{
    auto portoInstance = NContainers::CreatePortoInstance(
        GetUniqueName(),
        NContainers::CreatePortoExecutor());
    auto p = New<TPortoProcess>("/bin/bash", portoInstance, true);
    p->AddArgument("-c");
    p->AddArgument("if test 3 -gt 1; then exit 7; fi");

    auto error = WaitFor(p->Spawn());
    EXPECT_FALSE(error.IsOK());
    EXPECT_TRUE(p->IsFinished());
    portoInstance->Destroy();
}

TEST(TPortoProcessTest, Params2)
{
    auto portoInstance = NContainers::CreatePortoInstance(
        GetUniqueName(),
        NContainers::CreatePortoExecutor());
    auto p = New<TPortoProcess>("/bin/bash", portoInstance, true);
    p->AddArgument("-c");
    p->AddArgument("if test 1 -gt 3; then exit 7; fi");

    auto error = WaitFor(p->Spawn());
    EXPECT_TRUE(error.IsOK()) << ToString(error);
    EXPECT_TRUE(p->IsFinished());
    portoInstance->Destroy();
}

TEST(TPortoProcessTest, InheritEnvironment)
{
    const char* name = "SPAWN_TEST_ENV_VAR";
    const char* value = "42";
    setenv(name, value, 1);

    auto portoInstance = NContainers::CreatePortoInstance(
        GetUniqueName(),
        NContainers::CreatePortoExecutor());
    auto p = New<TPortoProcess>("/bin/bash", portoInstance, true);
    p->AddArgument("-c");
    p->AddArgument("if test $SPAWN_TEST_ENV_VAR = 42; then exit 7; fi");

    auto error = WaitFor(p->Spawn());
    EXPECT_FALSE(error.IsOK());
    EXPECT_TRUE(p->IsFinished());

    unsetenv(name);
    portoInstance->Destroy();
}

TEST(TPortoProcessTest, Kill)
{
    auto portoInstance = NContainers::CreatePortoInstance(
        GetUniqueName(),
        NContainers::CreatePortoExecutor());
    auto p = New<TPortoProcess>("/bin/sleep", portoInstance, true);
    p->AddArgument("5");

    auto finished = p->Spawn();

    NConcurrency::TDelayedExecutor::Submit(
        BIND([&] () {
            p->Kill(SIGKILL);
        }),
        TDuration::MilliSeconds(100));

    auto error = WaitFor(finished);
    EXPECT_FALSE(error.IsOK()) << ToString(error);
    EXPECT_TRUE(p->IsFinished());
    portoInstance->Destroy();
}

TEST(TPortoProcessTest, KillFinished)
{
    auto portoInstance = NContainers::CreatePortoInstance(
        GetUniqueName(),
        NContainers::CreatePortoExecutor());
    auto p = New<TPortoProcess>("/bin/bash", portoInstance, true);
    p->AddArgument("-c");
    p->AddArgument("true");

    auto finished = p->Spawn();

    auto error = WaitFor(finished);
    EXPECT_TRUE(error.IsOK());

    p->Kill(SIGKILL);
    portoInstance->Destroy();
}

TEST(TPortoProcessTest, PollDuration)
{
    auto portoInstance = NContainers::CreatePortoInstance(
        GetUniqueName(),
        NContainers::CreatePortoExecutor());
    auto p = New<TPortoProcess>("/bin/sleep", portoInstance, true, TDuration::MilliSeconds(1));
    p->AddArgument("1");

    auto error = WaitFor(p->Spawn());
    EXPECT_TRUE(error.IsOK()) << ToString(error);
    EXPECT_TRUE(p->IsFinished());
    portoInstance->Destroy();
}
////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

#endif
