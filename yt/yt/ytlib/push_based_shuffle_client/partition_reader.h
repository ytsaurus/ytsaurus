#pragma once

#include "public.h"
#include "record_format.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/memory/shared_range.h>

#include <functional>
#include <optional>

namespace NYT::NPushBasedShuffleClient {

////////////////////////////////////////////////////////////////////////////////

struct TShuffleReadRecord
{
    TRecordHeader Header;
    //! Holds the whole batch's row pool and every record's decompressed
    //! payload alive — including string/Any/Composite value bytes. Lifetime
    //! is per-batch, not per-record.
    TSharedRange<NTableClient::TUnversionedRow> Rows;
};

struct TShuffleReadBatch
    : public TRefCounted
{
    std::vector<TShuffleReadRecord> Records;

    //! True iff this batch is the last one. May be set on a non-empty batch,
    //! or on an empty trailing batch — both are valid.
    bool Finished = false;
};

DEFINE_REFCOUNTED_TYPE(TShuffleReadBatch)

////////////////////////////////////////////////////////////////////////////////

//! Optional record-level filter. Duplicate records are discarded before this
//! filter is applied.
using TRecordHeaderFilter = std::function<bool(const TRecordHeader& header)>;

struct IPushBasedPartitionReader
    : public virtual TRefCounted
{
    //! Drains up to MaxBytesPerRead from ready chunks. Cross-chunk order is
    //! unspecified. Deduplicates by (MapperId, StartRow) before decompression.
    //! An empty batch may have Finished=false.
    //!
    //! Reads must not overlap. Canceling a read cancels the reader; subsequent
    //! reads return the same error.
    [[nodiscard]] virtual TFuture<TShuffleReadBatchPtr> Read() = 0;

    //! Declare a new chunk. AddChunk after SetNoMoreChunks() or with a
    //! duplicate chunkId is a precondition violation. After a terminal
    //! error it no-ops (the caller only learns about that error via Read()).
    virtual void AddChunk(
        NChunkClient::TChunkId chunkId,
        NChunkClient::TChunkReplicaWithMediumList replicas,
        i64 startRecordIndex = 0,
        std::optional<i64> rangeEndRecordIndex = {}) = 0;

    //! Declare partition done. Idempotent; silently ignored after a terminal
    //! error.
    virtual void SetNoMoreChunks() = 0;
};

DEFINE_REFCOUNTED_TYPE(IPushBasedPartitionReader)

////////////////////////////////////////////////////////////////////////////////

IPushBasedPartitionReaderPtr CreatePushBasedPartitionReader(
    TPartitionReaderConfigPtr config,
    NApi::NNative::IClientPtr client,
    NChunkClient::TChunkReaderHostPtr chunkReaderHost,
    int readQuorum,
    IInvokerPtr invoker,
    TRecordHeaderFilter recordHeaderFilter = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPushBasedShuffleClient
