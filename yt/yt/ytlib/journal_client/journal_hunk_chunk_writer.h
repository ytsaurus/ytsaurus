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

struct TJournalHunkDescriptor
{
    NChunkClient::TChunkId ChunkId;

    i64 RecordIndex;
    i64 RecordOffset;

    i64 Length;

    NErasure::ECodec ErasureCodec;
    std::optional<i64> RecordSize;
};

////////////////////////////////////////////////////////////////////////////////

struct TJournalHunkChunkWriterStatistics
{
    i64 HunkCount = 0;

    i64 TotalSize = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IJournalHunkChunkWriter
    : public TRefCounted
{
    virtual TFuture<void> Open() = 0;
    virtual TFuture<void> Close() = 0;

    virtual TFuture<std::vector<TJournalHunkDescriptor>> WriteHunks(
        std::vector<TSharedRef> payloads) = 0;

    virtual TJournalHunkChunkWriterStatistics GetStatistics() const = 0;

    virtual bool IsCloseDemanded() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IJournalHunkChunkWriter)

////////////////////////////////////////////////////////////////////////////////

IJournalHunkChunkWriterPtr CreateJournalHunkChunkWriter(
    NApi::NNative::IClientPtr client,
    NChunkClient::TSessionId sessionId,
    TJournalHunkChunkWriterOptionsPtr options,
    TJournalHunkChunkWriterConfigPtr config,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
