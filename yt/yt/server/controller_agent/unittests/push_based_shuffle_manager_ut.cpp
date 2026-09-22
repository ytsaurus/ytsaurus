#include <yt/yt/server/controller_agent/config.h>
#include <yt/yt/server/controller_agent/push_based_shuffle_manager.h>

#include <yt/yt/ytlib/distributed_chunk_session_client/session_pool.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NControllerAgent {
namespace {

using namespace NDistributedChunkSessionClient;

////////////////////////////////////////////////////////////////////////////////

class TFakeSessionPool
    : public IDistributedChunkSessionPool
{
public:
    TFuture<TSessionDescriptor> GetSession(
        int /*slotCookie*/,
        std::optional<NChunkClient::TSessionId> /*excludedSessionId*/) override
    {
        YT_UNIMPLEMENTED();
    }

    void FinalizeSlot(int /*slotCookie*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<std::vector<TSlotChunkInfo>> GetSlotChunks(int /*slotCookie*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<std::vector<TReadySession>> GetReadySessions() const override
    {
        YT_UNIMPLEMENTED();
    }

    DEFINE_SIGNAL_OVERRIDE(void(const TSessionProgressUpdate& update), ProgressUpdated);
};

TPushBasedShuffleManagerPtr CreateManager()
{
    return New<TPushBasedShuffleManager>(New<TControllerAgentConfig>());
}

TIncarnationId MakeIncarnationId()
{
    return TIncarnationId(TGuid::Create());
}

TOperationId MakeOperationId()
{
    return TOperationId(TGuid::Create());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TPushBasedShuffleManagerTest, RejectsRequestWhileDisconnected)
{
    auto manager = CreateManager();
    auto operationId = MakeOperationId();

    EXPECT_THROW_WITH_ERROR_CODE(
        manager->GetShufflePoolOrThrow(MakeIncarnationId(), operationId),
        EErrorCode::AgentDisconnected);
    EXPECT_THROW_WITH_ERROR_CODE(
        manager->GetShufflePoolOrThrow(TIncarnationId(), operationId),
        EErrorCode::AgentDisconnected);
    EXPECT_THROW_WITH_ERROR_CODE(
        manager->RegisterShuffle(MakeIncarnationId(), operationId, New<TFakeSessionPool>()),
        EErrorCode::AgentDisconnected);
}

TEST(TPushBasedShuffleManagerTest, RejectsStaleIncarnation)
{
    auto manager = CreateManager();
    auto incarnationId = MakeIncarnationId();
    auto operationId = MakeOperationId();
    manager->OnSchedulerConnected(incarnationId);

    EXPECT_THROW_WITH_ERROR_CODE(
        manager->GetShufflePoolOrThrow(MakeIncarnationId(), operationId),
        EErrorCode::IncarnationMismatch);
    EXPECT_THROW_WITH_ERROR_CODE(
        manager->RegisterShuffle(MakeIncarnationId(), operationId, New<TFakeSessionPool>()),
        EErrorCode::IncarnationMismatch);
}

TEST(TPushBasedShuffleManagerTest, ReturnsRegisteredPool)
{
    auto manager = CreateManager();
    auto incarnationId = MakeIncarnationId();
    auto operationId = MakeOperationId();
    auto pool = New<TFakeSessionPool>();
    manager->OnSchedulerConnected(incarnationId);

    manager->RegisterShuffle(incarnationId, operationId, pool);

    EXPECT_EQ(manager->GetShufflePoolOrThrow(incarnationId, operationId), pool);
    EXPECT_THROW_WITH_ERROR_CODE(
        manager->GetShufflePoolOrThrow(incarnationId, MakeOperationId()),
        NRpc::EErrorCode::TransientFailure);
}

TEST(TPushBasedShuffleManagerTest, RejectsDuplicateRegistration)
{
    auto manager = CreateManager();
    auto incarnationId = MakeIncarnationId();
    auto operationId = MakeOperationId();
    auto pool = New<TFakeSessionPool>();
    manager->OnSchedulerConnected(incarnationId);

    manager->RegisterShuffle(incarnationId, operationId, pool);

    EXPECT_THROW_WITH_SUBSTRING(
        manager->RegisterShuffle(incarnationId, operationId, New<TFakeSessionPool>()),
        "already has a registered push-based shuffle");
    EXPECT_EQ(manager->GetShufflePoolOrThrow(incarnationId, operationId), pool);
}

TEST(TPushBasedShuffleManagerTest, ReportsExpiredPool)
{
    auto manager = CreateManager();
    auto incarnationId = MakeIncarnationId();
    auto operationId = MakeOperationId();
    manager->OnSchedulerConnected(incarnationId);

    manager->RegisterShuffle(incarnationId, operationId, New<TFakeSessionPool>());

    EXPECT_THROW_WITH_SUBSTRING(
        manager->GetShufflePoolOrThrow(incarnationId, operationId),
        "no longer has a shuffle session pool");
}

TEST(TPushBasedShuffleManagerTest, UnregisterDropsRegistration)
{
    auto manager = CreateManager();
    auto incarnationId = MakeIncarnationId();
    auto operationId = MakeOperationId();
    auto pool = New<TFakeSessionPool>();
    manager->OnSchedulerConnected(incarnationId);
    manager->RegisterShuffle(incarnationId, operationId, pool);

    manager->UnregisterShuffle(incarnationId, operationId);

    EXPECT_THROW_WITH_SUBSTRING(
        manager->GetShufflePoolOrThrow(incarnationId, operationId),
        "has no registered push-based shuffle");

    EXPECT_NO_THROW(manager->RegisterShuffle(incarnationId, operationId, pool));
}

TEST(TPushBasedShuffleManagerTest, IgnoresStaleUnregistration)
{
    auto manager = CreateManager();
    auto incarnationId = MakeIncarnationId();
    auto operationId = MakeOperationId();
    auto pool = New<TFakeSessionPool>();
    manager->OnSchedulerConnected(incarnationId);
    manager->RegisterShuffle(incarnationId, operationId, pool);

    manager->UnregisterShuffle(MakeIncarnationId(), operationId);

    EXPECT_EQ(manager->GetShufflePoolOrThrow(incarnationId, operationId), pool);
}

TEST(TPushBasedShuffleManagerTest, ReconnectDropsRegistrations)
{
    auto manager = CreateManager();
    auto incarnationId = MakeIncarnationId();
    auto operationId = MakeOperationId();
    auto pool = New<TFakeSessionPool>();
    manager->OnSchedulerConnected(incarnationId);
    manager->RegisterShuffle(incarnationId, operationId, pool);

    auto nextIncarnationId = MakeIncarnationId();
    manager->OnSchedulerConnected(nextIncarnationId);

    EXPECT_THROW_WITH_ERROR_CODE(
        manager->GetShufflePoolOrThrow(incarnationId, operationId),
        EErrorCode::IncarnationMismatch);
    EXPECT_THROW_WITH_SUBSTRING(
        manager->GetShufflePoolOrThrow(nextIncarnationId, operationId),
        "has no registered push-based shuffle");
}

TEST(TPushBasedShuffleManagerTest, CleanupDropsRegistrations)
{
    auto manager = CreateManager();
    auto incarnationId = MakeIncarnationId();
    auto operationId = MakeOperationId();
    auto pool = New<TFakeSessionPool>();
    manager->OnSchedulerConnected(incarnationId);
    manager->RegisterShuffle(incarnationId, operationId, pool);

    manager->Cleanup();

    EXPECT_THROW_WITH_ERROR_CODE(
        manager->GetShufflePoolOrThrow(incarnationId, operationId),
        EErrorCode::AgentDisconnected);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NControllerAgent
