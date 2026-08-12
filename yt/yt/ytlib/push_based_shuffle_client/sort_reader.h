#pragma once

#include "public.h"
#include "record_format.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/memory/shared_range.h>

#include <variant>

namespace NYT::NPushBasedShuffleClient {

////////////////////////////////////////////////////////////////////////////////

//! Sorts a partition or partition fragment in memory.
struct ISortReader
    : public virtual TRefCounted
{
    //! Returns the next sorted batch. Once an empty range is returned, all
    //! subsequent reads are empty.
    //!
    //! Calls must be serialized. If sorting is not finished, the call waits.
    //! Canceling it cancels the reader; later reads return the same error.
    [[nodiscard]] virtual TFuture<TSharedRange<NTableClient::TUnversionedRow>> Read() = 0;

    //! Adds input to the owned partition reader.
    virtual void AddChunk(
        NChunkClient::TChunkId chunkId,
        NChunkClient::TChunkReplicaWithMediumList replicas,
        i64 startRecordIndex = 0,
        std::optional<i64> rangeEndRecordIndex = {}) = 0;

    //! Declares that no additional chunks will be added to the owned partition reader.
    virtual void SetNoMoreChunks() = 0;

    //! Finishes the owned partition reader at the current committed record count.
    //! Must be called after SetNoMoreChunks().
    virtual void FinishAtCurrentCommittedRecordCount() = 0;
};

DEFINE_REFCOUNTED_TYPE(ISortReader)

////////////////////////////////////////////////////////////////////////////////

using TSortReaderMode = std::variant<TValidWriterIds, TIdentityColumnIds>;

//! TValidWriterIds selects identity-free mode; TIdentityColumnIds preserves identity.
//! Input keys occupy the first |comparator.GetLength()| values.
ISortReaderPtr CreateSortReader(
    TSortReaderConfigPtr sortReaderConfig,
    TPartitionReaderConfigPtr partitionReaderConfig,
    NApi::NNative::IClientPtr client,
    NChunkClient::TChunkReaderHostPtr chunkReaderHost,
    int readQuorum,
    NTableClient::TComparator comparator,
    TSortReaderMode mode,
    IInvokerPtr invoker,
    IInvokerPtr sortInvoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPushBasedShuffleClient
