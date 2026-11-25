#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/companion/client/companion_singleton_state.h>
#include <yt/yt/flow/library/cpp/companion/client/config.h>
#include <yt/yt/flow/library/cpp/companion/manager/companion_manager.h>
#include <yt/yt/flow/library/cpp/companion/manager/process_manager_base.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>
#include <yt/yt/flow/library/cpp/misc/testing/env_guard.h>

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/library/process/process.h>

#include <library/cpp/testing/common/network.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/system/env.h>
#include <util/system/fs.h>

namespace NYT::NFlow::NCompanion {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TPortTestProcessManager
    : public TProcessManagerBase
{
public:
    TPortTestProcessManager(IInvokerPtr invoker, bool fail)
        : TProcessManagerBase(invoker, nullptr, {}, TDuration::Seconds(1),
            TDuration::Hours(1), TDuration::Hours(1), TDuration::Hours(1),
            {}, {}, CreateSyncStatusProfiler())
        , Fail_(fail)
    { }

    TIntrusivePtr<TProcessBase> Process;

protected:
    void ValidateParameters() const override
    {
        if (Fail_) {
            THROW_ERROR_EXCEPTION("Test startup failure");
        }
    }

    TIntrusivePtr<TProcessBase> CreateProcessIncarnation(const TCompanionExecutionConfigPtr&) override
    {
        auto process = New<TSimpleProcess>("/bin/sleep");
        process->AddArgument("60");
        Process = process;
        return process;
    }

    TFuture<void> HealthCheck() override
    {
        return OKFuture;
    }

private:
    const bool Fail_;
};

class TPortTestManager
    : public TCompanionManager
{
public:
    TPortTestManager(TResourceContextPtr context, TDynamicResourceContextPtr dynamic, bool fail)
        : TCompanionManager(std::move(context), std::move(dynamic))
        , Fail_(fail)
    { }

    TIntrusivePtr<TPortTestProcessManager> ProcessManager;

protected:
    TProcessManagerBasePtr CreateProcessManager() override
    {
        ProcessManager = New<TPortTestProcessManager>(GetContext()->Invoker, Fail_);
        return ProcessManager;
    }

private:
    const bool Fail_;
};

class TCompanionPortOwnershipTest
    : public ::testing::Test
{
protected:
    const ::NTesting::TPortHolder Port_ = ::NTesting::GetFreePort();
    const TActionQueuePtr Queue_ = New<TActionQueue>("PortOwnerTest");

    void SetUp() override
    {
        PreviousConfig_ = GetCompanionExecutionConfig();
        auto config = New<TCompanionExecutionConfig>();
        config->Port = static_cast<int>(Port_);
        SetCompanionExecutionConfig(config);
    }

    void TearDown() override
    {
        SetCompanionExecutionConfig(PreviousConfig_);
    }

    TIntrusivePtr<TPortTestManager> MakeManager(bool fail = false)
    {
        auto context = New<TResourceContext>();
        context->ResourceId = "companion";
        context->ResourceInstanceId = TResourceInstanceId(TGuid::Create());
        context->ResourceSpec = New<TResourceSpec>();
        context->ResourceSpec->ResourceClassName = "NYT::NFlow::NCompanion::TCompanionManager";
        context->Invoker = Queue_->GetInvoker();
        context->StatusProfiler = CreateSyncStatusProfiler();
        auto dynamic = New<TDynamicResourceContext>();
        dynamic->DynamicResourceSpec = New<TDynamicResourceSpec>();
        return New<TPortTestManager>(context, dynamic, fail);
    }

    static void DestroyManager(TIntrusivePtr<TPortTestManager>& manager)
    {
        auto destroyed = manager->GetDestroyedFuture();
        manager.Reset();
        WaitFor(destroyed.WithTimeout(TDuration::Seconds(5))).ThrowOnError();
    }

private:
    TCompanionExecutionConfigPtr PreviousConfig_;
};

TEST_W(TCompanionPortOwnershipTest, SequentialLoadsReusePendingAndSuccessfulStartup)
{
    auto manager = MakeManager();
    WaitFor(BIND([&] {
        auto started = manager->Load({});
        EXPECT_EQ(started, manager->Load({}));
        auto onSessionFinished = BIND([] (const std::string&) {
        });
        manager->SubscribeSessionFinished(onSessionFinished);
        EXPECT_TRUE(manager->GetSessionId().empty());
        manager->UnsubscribeSessionFinished(onSessionFinished);
        return started;
    })
            .AsyncVia(Queue_->GetInvoker())
            .Run())
        .ThrowOnError();

    auto session = manager->GetSessionId();
    EXPECT_FALSE(session.empty());
    WaitFor(manager->Load({})).ThrowOnError();
    EXPECT_EQ(session, manager->GetSessionId());
}

TEST_W(TCompanionPortOwnershipTest, RejectedManagerPreservesOwnerAndCanRetryAfterRelease)
{
    // Record port cleanup attempts, including probes that find no process.
    TTempDir directory;
    auto lsof = directory.Path() / "lsof";
    auto probes = directory.Path() / "probes";
    {
        TFileOutput script(lsof.GetPath());
        script << "#!/bin/sh\nprintf '%s\\n' \"$1\" >> \"${0%/*}/probes\"\n";
        script.Finish();
    }
    ASSERT_TRUE(NFs::SetExecutable(lsof.GetPath(), true));
    NTesting::TEnvGuard pathGuard("PATH", Format("%v:%v", directory.Name(), GetEnv("PATH")));

    auto owner = MakeManager();
    WaitFor(owner->Load({})).ThrowOnError();
    auto process = owner->ProcessManager->Process;
    WaitForPredicate([&] {
        return process->IsStarted();
    });
    auto expectedProbe = Format("-ti:%v\n", static_cast<int>(Port_));
    ASSERT_TRUE(probes.Exists());
    ASSERT_EQ(expectedProbe, TUnbufferedFileInput(probes.GetPath()).ReadAll());

    auto duplicate = MakeManager();
    EXPECT_THROW_WITH_SUBSTRING(WaitFor(duplicate->Load({})).ThrowOnError(), "Another loaded companion manager");
    DestroyManager(duplicate);

    auto replacement = MakeManager();
    EXPECT_THROW_WITH_SUBSTRING(WaitFor(replacement->Load({})).ThrowOnError(), "Another loaded companion manager");
    EXPECT_FALSE(replacement->ProcessManager->Process);
    EXPECT_EQ(expectedProbe, TUnbufferedFileInput(probes.GetPath()).ReadAll());
    EXPECT_FALSE(process->IsFinished());

    DestroyManager(owner);
    WaitForPredicate([&] {
        return process->IsFinished();
    });
    WaitFor(replacement->Load({})).ThrowOnError();
    EXPECT_TRUE(replacement->ProcessManager->Process);
    EXPECT_EQ(expectedProbe + expectedProbe, TUnbufferedFileInput(probes.GetPath()).ReadAll());
}

TEST_W(TCompanionPortOwnershipTest, StartupFailureReleasesPort)
{
    auto replacement = MakeManager();
    auto failed = MakeManager(/*fail*/ true);
    EXPECT_THROW_WITH_SUBSTRING(WaitFor(failed->Load({})).ThrowOnError(), "Test startup failure");
    WaitFor(replacement->Load({})).ThrowOnError();
    EXPECT_TRUE(replacement->ProcessManager->Process);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanion
