#pragma once

#include "public.h"

#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/ytlib/table_client/hunks.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>

#include <yt/yt/core/misc/ref.h>
#include <yt/yt/core/misc/range.h>

#include <yt/yt/core/actions/future.h>

#include <variant>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IHunkChunkPayloadWriter
    : public virtual TRefCounted
{
    //! Enqueues a given #payload for writing.
    //! Returns an offset and a flag indicating if the caller must wait on
    //! #GetReadyEvent before proceeding any further.
    virtual std::tuple<i64, bool> WriteHunk(TRef payload) = 0;

    //! Returns |true| if some hunks were added via #WriteHunk.
    virtual bool HasHunks() const = 0;

    //! See #WriteHunk.
    virtual TFuture<void> GetReadyEvent() = 0;

    //! Returns the future that is set when the chunk becomes open.
    //! At least one hunk must have been added via #WriteHunk prior to this call.
    virtual TFuture<void> GetOpenFuture() = 0;

    //! Flushes and closes the writer (both this and the underlying one).
    //! If no hunks were added via #WriteHunk, returns #VoidFuture.
    virtual TFuture<void> Close() = 0;

    //! Returns the chunk meta. The chunk must be already closed, see #Close.
    virtual NChunkClient::TDeferredChunkMetaPtr GetMeta() const = 0;

    //! Returns the chunk id. The chunk must be already open, see #GetOpenFuture.
    virtual NChunkClient::TChunkId GetChunkId() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IHunkChunkPayloadWriter)

IHunkChunkPayloadWriterPtr CreateHunkChunkPayloadWriter(
    THunkChunkPayloadWriterConfigPtr config,
    NChunkClient::IChunkWriterPtr underlying);

////////////////////////////////////////////////////////////////////////////////

//! Reads hunks in #rows and decodes them (updating #rows in-place).
//! May return null if no asynchronous activities are needed.
template <class TRow>
TFuture<TSharedRange<TRow>> ReadAndDecodeHunks(
    NChunkClient::IChunkFragmentReaderPtr chunkFragmentReader,
    NTableClient::TTableSchemaPtr schema,
    NChunkClient::TClientChunkReadOptions options,
    TSharedRange<TRow> rows);

//! Constructs a writer performing hunk encoding.
//! Encoded rows are written to #underlying, hunks go to #hunkChunkPayloadWriter.
//! If #schema does not contain hunk columns then #underlying is returned as is.
NTableClient::IVersionedChunkWriterPtr CreateHunkEncodingVersionedWriter(
    NTableClient::IVersionedChunkWriterPtr underlying,
    NTableClient::TTableSchemaPtr schema,
    IHunkChunkPayloadWriterPtr hunkChunkPayloadWriter);

//! Constructs a reader replacing hunk refs with their content
//! (obtained by reading it via #chunkFragmentReader).
//! If #schema does not contain hunk columns then #underlying is returned as is.
NTableClient::ISchemafulUnversionedReaderPtr CreateHunkDecodingSchemafulReader(
    TBatchHunkReaderConfigPtr config,
    NTableClient::ISchemafulUnversionedReaderPtr underlying,
    NChunkClient::IChunkFragmentReaderPtr chunkFragmentReader,
    NTableClient::TTableSchemaPtr schema,
    NChunkClient::TClientChunkReadOptions options);

//! Constructs a reader replacing hunk refs with inline hunks
//! (obtained by reading these via #chunkFragmentReader).
//! This inlining happens for hunks smaller than |MaxInlineHunkSize|
//! and is also forced for all hunks contained in chunks with ids from #hunkChunkIdsToForceInline.
//! If #schema does not contain hunk columns then #underlying is returned as is.
NTableClient::IVersionedReaderPtr CreateHunkInliningVersionedReader(
    TBatchHunkReaderConfigPtr config,
    NTableClient::IVersionedReaderPtr underlying,
    NChunkClient::IChunkFragmentReaderPtr chunkFragmentReader,
    NTableClient::TTableSchemaPtr schema,
    THashSet<NChunkClient::TChunkId> hunkChunkIdsToForceInline,
    NChunkClient::TClientChunkReadOptions options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
