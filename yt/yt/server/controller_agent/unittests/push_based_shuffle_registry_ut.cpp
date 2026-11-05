#include <yt/yt/server/controller_agent/push_based_shuffle_registry.h>

#include <yt/yt/ytlib/distributed_chunk_session_client/session_pool.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/rpc/public.h>

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

    std::vector<TReadySession> GetReadySessions() const override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<void> Finalize() override
    {
        YT_UNIMPLEMENTED();
    }

    DEFINE_SIGNAL_OVERRIDE(void(const TSessionProgressUpdate& update), ProgressUpdated);
};

TPushBasedShuffleRegistryPtr CreateRegistry()
{
    return New<TPushBasedShuffleRegistry>();
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

TEST(TPushBasedShuffleRegistryTest, RejectsRequestWhileDisconnected)
{
    auto registry = CreateRegistry();
    auto operationId = MakeOperationId();

    EXPECT_THROW_WITH_ERROR_CODE(
        registry->GetShufflePoolOrThrow(MakeIncarnationId(), operationId),
        EErrorCode::AgentDisconnected);
    EXPECT_THROW_WITH_ERROR_CODE(
        registry->GetShufflePoolOrThrow(TIncarnationId(), operationId),
        EErrorCode::AgentDisconnected);
    EXPECT_THROW_WITH_ERROR_CODE(
        registry->RegisterShuffle(MakeIncarnationId(), operationId, New<TFakeSessionPool>()),
        EErrorCode::AgentDisconnected);
}

TEST(TPushBasedShuffleRegistryTest, RejectsStaleIncarnation)
{
    auto registry = CreateRegistry();
    auto incarnationId = MakeIncarnationId();
    auto operationId = MakeOperationId();
    registry->OnSchedulerConnected(incarnationId);

    EXPECT_THROW_WITH_ERROR_CODE(
        registry->GetShufflePoolOrThrow(MakeIncarnationId(), operationId),
        EErrorCode::IncarnationMismatch);
    EXPECT_THROW_WITH_ERROR_CODE(
        registry->RegisterShuffle(MakeIncarnationId(), operationId, New<TFakeSessionPool>()),
        EErrorCode::IncarnationMismatch);
}

TEST(TPushBasedShuffleRegistryTest, ReturnsRegisteredPool)
{
    auto registry = CreateRegistry();
    auto incarnationId = MakeIncarnationId();
    auto operationId = MakeOperationId();
    auto pool = New<TFakeSessionPool>();
    registry->OnSchedulerConnected(incarnationId);

    registry->RegisterShuffle(incarnationId, operationId, pool);

    EXPECT_EQ(registry->GetShufflePoolOrThrow(incarnationId, operationId), pool);
    EXPECT_THROW_WITH_ERROR_CODE(
        registry->GetShufflePoolOrThrow(incarnationId, MakeOperationId()),
        NRpc::EErrorCode::TransientFailure);
}

TEST(TPushBasedShuffleRegistryTest, RejectsDuplicateRegistration)
{
    auto registry = CreateRegistry();
    auto incarnationId = MakeIncarnationId();
    auto operationId = MakeOperationId();
    auto pool = New<TFakeSessionPool>();
    registry->OnSchedulerConnected(incarnationId);

    registry->RegisterShuffle(incarnationId, operationId, pool);

    EXPECT_THROW_WITH_SUBSTRING(
        registry->RegisterShuffle(incarnationId, operationId, New<TFakeSessionPool>()),
        "already has a registered push-based shuffle");
    EXPECT_EQ(registry->GetShufflePoolOrThrow(incarnationId, operationId), pool);
}

TEST(TPushBasedShuffleRegistryTest, ReportsExpiredPool)
{
    auto registry = CreateRegistry();
    auto incarnationId = MakeIncarnationId();
    auto operationId = MakeOperationId();
    registry->OnSchedulerConnected(incarnationId);

    registry->RegisterShuffle(incarnationId, operationId, New<TFakeSessionPool>());

    EXPECT_THROW_WITH_SUBSTRING(
        registry->GetShufflePoolOrThrow(incarnationId, operationId),
        "no longer has a shuffle session pool");
}

TEST(TPushBasedShuffleRegistryTest, UnregisterDropsRegistration)
{
    auto registry = CreateRegistry();
    auto incarnationId = MakeIncarnationId();
    auto operationId = MakeOperationId();
    auto pool = New<TFakeSessionPool>();
    registry->OnSchedulerConnected(incarnationId);
    registry->RegisterShuffle(incarnationId, operationId, pool);

    registry->UnregisterShuffle(incarnationId, operationId);

    EXPECT_THROW_WITH_SUBSTRING(
        registry->GetShufflePoolOrThrow(incarnationId, operationId),
        "has no registered push-based shuffle");

    EXPECT_NO_THROW(registry->RegisterShuffle(incarnationId, operationId, pool));
}

TEST(TPushBasedShuffleRegistryTest, IgnoresStaleUnregistration)
{
    auto registry = CreateRegistry();
    auto incarnationId = MakeIncarnationId();
    auto operationId = MakeOperationId();
    auto pool = New<TFakeSessionPool>();
    registry->OnSchedulerConnected(incarnationId);
    registry->RegisterShuffle(incarnationId, operationId, pool);

    registry->UnregisterShuffle(MakeIncarnationId(), operationId);

    EXPECT_EQ(registry->GetShufflePoolOrThrow(incarnationId, operationId), pool);
}

TEST(TPushBasedShuffleRegistryTest, ReconnectDropsRegistrations)
{
    auto registry = CreateRegistry();
    auto incarnationId = MakeIncarnationId();
    auto operationId = MakeOperationId();
    auto pool = New<TFakeSessionPool>();
    registry->OnSchedulerConnected(incarnationId);
    registry->RegisterShuffle(incarnationId, operationId, pool);

    auto nextIncarnationId = MakeIncarnationId();
    registry->OnSchedulerConnected(nextIncarnationId);

    EXPECT_THROW_WITH_ERROR_CODE(
        registry->GetShufflePoolOrThrow(incarnationId, operationId),
        EErrorCode::IncarnationMismatch);
    EXPECT_THROW_WITH_SUBSTRING(
        registry->GetShufflePoolOrThrow(nextIncarnationId, operationId),
        "has no registered push-based shuffle");
}

TEST(TPushBasedShuffleRegistryTest, CleanupDropsRegistrations)
{
    auto registry = CreateRegistry();
    auto incarnationId = MakeIncarnationId();
    auto operationId = MakeOperationId();
    auto pool = New<TFakeSessionPool>();
    registry->OnSchedulerConnected(incarnationId);
    registry->RegisterShuffle(incarnationId, operationId, pool);

    registry->Cleanup();

    EXPECT_THROW_WITH_ERROR_CODE(
        registry->GetShufflePoolOrThrow(incarnationId, operationId),
        EErrorCode::AgentDisconnected);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NControllerAgent
