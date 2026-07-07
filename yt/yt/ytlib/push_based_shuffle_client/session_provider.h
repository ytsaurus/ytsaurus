#pragma once

#include "public.h"

#include <yt/yt/ytlib/distributed_chunk_session_client/distributed_chunk_session_pool.h>

#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/core/actions/future.h>

#include <optional>
#include <vector>

namespace NYT::NPushBasedShuffleClient {

////////////////////////////////////////////////////////////////////////////////

//! Mapper-side facade over the controller-agent-owned pool. Implementations
//! translate a partition index to a slot cookie internally; the mapper-side
//! caller never sees slot cookies.
struct IPartitionWriteSessionProvider
    : public virtual TRefCounted
{
    virtual TFuture<NDistributedChunkSessionClient::TSessionDescriptor> GetSession(
        int partitionIndex,
        std::optional<NChunkClient::TSessionId> excludedSessionId = {}) = 0;
};

DEFINE_REFCOUNTED_TYPE(IPartitionWriteSessionProvider)

////////////////////////////////////////////////////////////////////////////////

//! Trivial in-process provider that delegates to a pool using a fixed
//! partition_index -> slot_cookie mapping. partitionToSlotCookie.size()
//! determines the supported partition range.
IPartitionWriteSessionProviderPtr CreateDirectPartitionWriteSessionProvider(
    NDistributedChunkSessionClient::IDistributedChunkSessionPoolPtr pool,
    std::vector<int> partitionToSlotCookie);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPushBasedShuffleClient
