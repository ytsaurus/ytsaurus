#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/library/erasure/public.h>

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
    virtual TFuture<void> WriteRecord(TSharedRef record) = 0;
    //! Only for erasure chunk writer.
    //! Writes #recordParts as-is to corresponding nodes without performing erasure encoding.
    //! Size of #recordParts must coinside with number of nodes (i.e. total part count).
    virtual TFuture<void> WriteEncodedRecordParts(std::vector<TSharedRef> recordParts) = 0;

    virtual bool IsCloseDemanded() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IJournalChunkWriter)

////////////////////////////////////////////////////////////////////////////////

IJournalChunkWriterPtr CreateJournalChunkWriter(
    NApi::NNative::IClientPtr client,
    NChunkClient::TSessionId sessionId,
    NApi::TJournalChunkWriterOptionsPtr options,
    NApi::TJournalChunkWriterConfigPtr config,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
