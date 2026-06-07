#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/distributed_chunk_session_client/distributed_chunk_session_pool.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/public.h>

#include <library/cpp/yt/memory/range.h>

namespace NYT::NPushBasedShuffleClient {

////////////////////////////////////////////////////////////////////////////////

struct IPushBasedShuffleWriter
    : public virtual TRefCounted
{
    //! Routes rows to per-partition builders and ships compressed records.
    //! The future resolves when the batch is accepted and backpressure
    //! (if any) clears. Caller must serialize calls, keep `rows` alive
    //! until the future resolves, and not call after Close.
    //!
    //! v1 limitation: within one Write call, in-flight bytes can transiently
    //! exceed `InFlightBudget` by ~the compressed size of the batch's evicted
    //! records. Backpressure between Write calls is enforced as normal.
    //! Callers wanting a strict memory cap should bound batch size accordingly.
    [[nodiscard]] virtual TFuture<void> Write(TRange<NTableClient::TUnversionedRow> rows) = 0;

    //! Flush all builders, drain in-flight + pending records. Resolves when
    //! every record submitted before Close has been quorum-acked.
    [[nodiscard]] virtual TFuture<void> Close() = 0;
};

DEFINE_REFCOUNTED_TYPE(IPushBasedShuffleWriter)

////////////////////////////////////////////////////////////////////////////////

IPushBasedShuffleWriterPtr CreatePushBasedShuffleWriter(
    TShuffleWriterConfigPtr config,
    IPartitionWriteSessionProviderPtr sessionProvider,
    NTableClient::IPartitionerPtr partitioner,
    NApi::NNative::IConnectionPtr connection,
    i32 mapperId,
    IInvokerPtr invoker,
    THashMap<int, NDistributedChunkSessionClient::TSessionDescriptor> seededSessions = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPushBasedShuffleClient
