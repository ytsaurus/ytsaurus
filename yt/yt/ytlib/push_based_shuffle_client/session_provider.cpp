#include "session_provider.h"

namespace NYT::NPushBasedShuffleClient {

using namespace NChunkClient;
using namespace NDistributedChunkSessionClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDirectPartitionWriteSessionProvider
    : public IPartitionWriteSessionProvider
{
public:
    TDirectPartitionWriteSessionProvider(
        IDistributedChunkSessionPoolPtr pool,
        std::vector<int> partitionToSlotCookie)
        : Pool_(std::move(pool))
        , PartitionToSlotCookie_(std::move(partitionToSlotCookie))
    { }

    TFuture<TSessionDescriptor> GetSession(
        int partitionIndex,
        std::optional<TSessionId> excludedSessionId) final
    {
        YT_VERIFY(partitionIndex >= 0);
        YT_VERIFY(partitionIndex < std::ssize(PartitionToSlotCookie_));
        return Pool_->GetSession(
            PartitionToSlotCookie_[partitionIndex],
            excludedSessionId);
    }

private:
    const IDistributedChunkSessionPoolPtr Pool_;
    const std::vector<int> PartitionToSlotCookie_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

IPartitionWriteSessionProviderPtr CreateDirectPartitionWriteSessionProvider(
    IDistributedChunkSessionPoolPtr pool,
    std::vector<int> partitionToSlotCookie)
{
    return New<TDirectPartitionWriteSessionProvider>(
        std::move(pool),
        std::move(partitionToSlotCookie));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPushBasedShuffleClient
