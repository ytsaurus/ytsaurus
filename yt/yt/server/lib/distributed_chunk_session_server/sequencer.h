#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NDistributedChunkSessionServer {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  \note Thread affinity: any
 */
struct IDistributedChunkSessionSequencer
    : virtual public TRefCounted
{
    virtual TFuture<void> Open() = 0;

    virtual TFuture<void> WriteRecord(TSharedRef record) = 0;

    virtual TFuture<void> GetClosedFuture() = 0;

    virtual TFuture<void> Close() = 0;
};

DEFINE_REFCOUNTED_TYPE(IDistributedChunkSessionSequencer)

////////////////////////////////////////////////////////////////////////////////

IDistributedChunkSessionSequencerPtr CreateDistributedChunkSessionSequencer(
    NChunkClient::TSessionId sessionId,
    NChunkClient::TChunkReplicaWithMediumList targets,
    NApi::TJournalChunkWriterOptionsPtr options,
    NApi::TJournalChunkWriterConfigPtr config,
    NApi::NNative::IConnectionPtr connection,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionServer
