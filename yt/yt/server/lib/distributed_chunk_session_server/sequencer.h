#pragma once

#include "public.h"

#include <yt/yt/ytlib/distributed_chunk_session_client/statistics.h>

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

    //! Submission failures are reported through the returned future, never by throwing.
    virtual TFuture<void> WriteRecord(
        TSharedRef record,
        NDistributedChunkSessionClient::TDistributedChunkSessionWriteStatistics statistics) noexcept = 0;

    //! Returns the progress of the longest quorum-confirmed contiguous record prefix.
    virtual NDistributedChunkSessionClient::TDistributedChunkSessionProgress GetProgress() const = 0;

    virtual TFuture<void> GetClosedFuture() = 0;

    //! Must not be called before Open().
    virtual TFuture<NDistributedChunkSessionClient::TDistributedChunkSessionProgress> Close() = 0;
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
