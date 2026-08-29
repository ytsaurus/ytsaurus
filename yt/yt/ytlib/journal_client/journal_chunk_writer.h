#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/library/erasure/public.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/logging/log.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NJournalClient {

////////////////////////////////////////////////////////////////////////////////

struct IJournalChunkWriter
    : public TRefCounted
{
    virtual TFuture<void> Open() = 0;
    virtual TFuture<void> Close() = 0;

    //! Writes #record to nodes. Performs its erasure encoding beforehand if needed.
    //! The returned future is set with the record's index once it reaches the write quorum.
    //! Submission failures are reported through the returned future, never by throwing,
    //! so a caller may account for the record before submitting it.
    virtual TFuture<i64> WriteRecord(TSharedRef record) noexcept = 0;

    //! Only for erasure chunk writer.
    //! Writes #recordParts as-is to corresponding nodes without performing erasure encoding.
    //! Size of #recordParts must coinside with number of nodes (i.e. total part count).
    //! Submission failures are reported through the returned future, never by throwing,
    //! so a caller may account for the record before submitting it.
    virtual TFuture<void> WriteEncodedRecordParts(std::vector<TSharedRef> recordParts) noexcept = 0;

    virtual bool IsCloseDemanded() const = 0;

    //! The replicas the writer targets, i.e. the ones holding the records it wrote.
    /*!
     *  Only valid once #Open has succeeded; the targets do not change afterwards, so this is safe to call
     *  from any thread and remains valid even after the writer fails.
     */
    virtual std::vector<TChunkReplicaDescriptor> GetChunkReplicaDescriptors() const = 0;

    //! Fired (once) when the writer fails; the writer must not be used afterwards.
    DECLARE_INTERFACE_SIGNAL(void(const TError&), Failed);
};

DEFINE_REFCOUNTED_TYPE(IJournalChunkWriter)

////////////////////////////////////////////////////////////////////////////////

IJournalChunkWriterPtr CreateJournalChunkWriter(
    NApi::NNative::IClientPtr client,
    NChunkClient::TSessionId sessionId,
    NApi::TJournalChunkWriterOptionsPtr options,
    NApi::TJournalChunkWriterConfigPtr config,
    NApi::TJournalWriterPerformanceCounters counters,
    IInvokerPtr invoker,
    std::optional<NChunkClient::TChunkReplicaWithMediumList> targets,
    NChunkClient::EChunkFormat chunkFormat,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
