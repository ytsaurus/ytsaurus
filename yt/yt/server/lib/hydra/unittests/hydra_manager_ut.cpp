#include <yt/yt/server/lib/hydra/changelog.h>
#include <yt/yt/server/lib/hydra/composite_automaton.h>
#include <yt/yt/server/lib/hydra/config.h>
#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>
#include <yt/yt/server/lib/hydra/public.h>
#include <yt/yt/server/lib/hydra/serialize.h>
#include <yt/yt/server/lib/hydra/snapshot.h>

#include <yt/yt/server/lib/election/election_manager.h>

#include <yt/yt/client/hydra/version.h>
#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/rpc/authenticator.h>
#include <yt/yt/core/rpc/local_server.h>
#include <yt/yt/core/test_framework/framework.h>

#include <atomic>

namespace NYT::NHydra {
namespace {

using namespace NConcurrency;
using namespace NElection;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TAutomatonStub
    : public TCompositeAutomaton
{
public:
    TAutomatonStub()
        : TCompositeAutomaton(nullptr, TCellId())
    { }

    TReign GetCurrentReign() override
    {
        return 1;
    }

    EFinalRecoveryAction GetActionToRecoverFromReign(TReign /*reign*/) override
    {
        return EFinalRecoveryAction::None;
    }

protected:
    std::unique_ptr<TSaveContext> CreateSaveContext(
        ICheckpointableOutputStream* output,
        NLogging::TLogger logger) override
    {
        return std::make_unique<TSaveContext>(output, std::move(logger), GetCurrentReign());
    }

    std::unique_ptr<TLoadContext> CreateLoadContext(
        ICheckpointableInputStream* input) override
    {
        auto context = std::make_unique<TLoadContext>(input);
        TCompositeAutomaton::SetupLoadContext(context.get());
        return context;
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Fails the first #GetLatestSnapshotId calls with the preconfigured errors
//! and then reports an empty snapshot store.
class TSnapshotStoreStub
    : public ISnapshotStore
{
public:
    explicit TSnapshotStoreStub(std::vector<TError> errors)
        : Errors_(std::move(errors))
    { }

    TFuture<int> GetLatestSnapshotId(int /*maxSnapshotId*/) override
    {
        if (NextError_ < static_cast<int>(Errors_.size())) {
            return MakeFuture<int>(Errors_[NextError_++]);
        }
        return MakeFuture(InvalidSegmentId);
    }

    ISnapshotReaderPtr CreateReader(int /*snapshotId*/) override
    {
        YT_UNIMPLEMENTED();
    }

    ISnapshotWriterPtr CreateWriter(int /*snapshotId*/, const NProto::TSnapshotMeta& /*meta*/) override
    {
        YT_UNIMPLEMENTED();
    }

private:
    const std::vector<TError> Errors_;
    int NextError_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TChangelogStoreStub
    : public IChangelogStore
{
public:
    bool IsReadOnly() const override
    {
        return false;
    }

    std::optional<int> TryGetTerm() const override
    {
        return 0;
    }

    TFuture<void> SetTerm(int /*term*/) override
    {
        YT_UNIMPLEMENTED();
    }

    std::optional<int> TryGetLatestChangelogId() const override
    {
        return std::nullopt;
    }

    std::optional<TElectionPriority> GetElectionPriority() const override
    {
        return TElectionPriority();
    }

    std::optional<TVersion> GetReachableVersion() const override
    {
        return std::nullopt;
    }

    TFuture<IChangelogPtr> CreateChangelog(
        int /*id*/,
        const NProto::TChangelogMeta& /*meta*/,
        const TChangelogOptions& /*options*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<IChangelogPtr> OpenChangelog(
        int /*id*/,
        const TChangelogOptions& /*options*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<void> RemoveChangelog(int /*id*/) override
    {
        YT_UNIMPLEMENTED();
    }

    void Abort() override
    { }
};

////////////////////////////////////////////////////////////////////////////////

class TChangelogStoreFactoryStub
    : public IChangelogStoreFactory
{
public:
    TFuture<IChangelogStorePtr> Lock() override
    {
        return MakeFuture<IChangelogStorePtr>(New<TChangelogStoreStub>());
    }

    void Reconfigure(const TDynamicRemoteChangelogStoreConfigPtr& /*config*/) override
    { }
};

////////////////////////////////////////////////////////////////////////////////

class TElectionManagerStub
    : public IElectionManager
{
public:
    void Initialize() override
    { }

    void Finalize() override
    { }

    void Participate() override
    {
        Participated_.store(true);
    }

    TFuture<void> Abandon(const TError& /*error*/) override
    {
        YT_UNIMPLEMENTED();
    }

    void ReconfigureCell(TCellManagerPtr /*cellManager*/) override
    {
        YT_UNIMPLEMENTED();
    }

    NYson::TYsonProducer GetMonitoringProducer() override
    {
        YT_UNIMPLEMENTED();
    }

    TPeerIdSet GetAlivePeerIds() override
    {
        YT_UNIMPLEMENTED();
    }

    TCellManagerPtr GetCellManager() override
    {
        YT_UNIMPLEMENTED();
    }

    void Discombobulate() override
    {
        YT_UNIMPLEMENTED();
    }

    bool GetParticipated() const
    {
        return Participated_.load();
    }

private:
    std::atomic<bool> Participated_ = false;
};

////////////////////////////////////////////////////////////////////////////////

//! Runs the hydra manager with a snapshot store failing with #errors
//! and returns the number of #PrerequisiteTransactionAborted signals raised.
int RunParticipation(int errorCount, TError error)
{
    auto config = New<TDistributedHydraManagerConfig>();
    config->MinPersistentStoreInitializationBackoffTime = TDuration::MilliSeconds(1);
    config->MaxPersistentStoreInitializationBackoffTime = TDuration::MilliSeconds(1);

    auto threadPool = CreateThreadPool(1, "HydraManager");
    auto controlInvoker = threadPool->GetInvoker();

    std::vector<TError> errors;
    for (int index = 0; index < errorCount; ++index) {
        errors.push_back(error);
    }

    auto electionManager = New<TElectionManagerStub>();
    auto hydraManager = CreateDistributedHydraManager(
        config,
        controlInvoker,
        controlInvoker,
        New<TAutomatonStub>(),
        CreateLocalServer(),
        electionManager,
        TCellId(),
        1,
        New<TChangelogStoreFactoryStub>(),
        New<TSnapshotStoreStub>(std::move(errors)),
        CreateNoopAuthenticator());

    std::atomic<int> abortedSignalCount = 0;
    hydraManager->SubscribePrerequisiteTransactionAborted(BIND([&] {
        ++abortedSignalCount;
    }));

    WaitFor(BIND(&IHydraManager::Initialize, hydraManager).AsyncVia(controlInvoker).Run())
        .ThrowOnError();

    WaitForPredicate([&] {
        return electionManager->GetParticipated();
    });

    return abortedSignalCount;
}

TEST(THydraManagerTest, PrerequisiteTransactionAbortedSignalIsRaised)
{
    auto abortedSignalCount = RunParticipation(
        1,
        TError(NTransactionClient::EErrorCode::NoSuchTransaction, "Prerequisite transaction is gone"));

    EXPECT_EQ(abortedSignalCount, 1);
}

TEST(THydraManagerTest, PrerequisiteTransactionAbortedSignalIsRaisedOnRetry)
{
    auto abortedSignalCount = RunParticipation(
        3,
        TError(NTransactionClient::EErrorCode::NoSuchTransaction, "Prerequisite transaction is gone"));

    EXPECT_EQ(abortedSignalCount, 3);
}

TEST(THydraManagerTest, OtherErrorsDoNotRaiseSignal)
{
    auto abortedSignalCount = RunParticipation(
        1,
        TError("Persistent store initialization failed"));

    EXPECT_EQ(abortedSignalCount, 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NHydra
