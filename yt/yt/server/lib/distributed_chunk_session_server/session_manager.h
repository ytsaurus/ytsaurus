#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/session_id.h>

namespace NYT::NDistributedChunkSessionServer {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  \note Thread affinity: any
 */
struct IDistributedChunkSessionManager
    : virtual public TRefCounted
{
    virtual IDistributedChunkSessionSequencerPtr GetSequencerOrThrow(NChunkClient::TSessionId sessionId) const = 0;

    virtual TFuture<void> StartSession(
        NChunkClient::TSessionId sessionId,
        TDuration sessionTimeout,
        NChunkClient::TChunkReplicaWithMediumList targets,
        NApi::TJournalChunkWriterOptionsPtr options,
        NApi::TJournalChunkWriterConfigPtr config) = 0;

    virtual void RenewSessionLease(NChunkClient::TSessionId sessionId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDistributedChunkSessionManager)

////////////////////////////////////////////////////////////////////////////////

IDistributedChunkSessionManagerPtr CreateDistributedChunkSessionManager(
    IInvokerPtr invoker,
    NApi::NNative::IConnectionPtr connection);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionServer
