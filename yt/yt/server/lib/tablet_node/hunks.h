#pragma once

#include "public.h"

#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/ytlib/table_client/hunks.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/misc/ref.h>
#include <yt/yt/core/misc/range.h>

#include <yt/yt/core/actions/future.h>

#include <variant>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IHunkChunkPayloadWriter
    : public virtual TRefCounted
{
    //! Opens the writer.
    virtual TFuture<void> Open() = 0;

    //! Enqueues a given #payload for writing.
    //! Returns a local ref and a flag indicating if the caller must wait on
    //! #GetReadyEvent before proceeding any further.
    virtual std::tuple<NTableClient::TLocalRefHunkValue, bool> WriteHunk(TRef payload) = 0;

    //! See #WriteHunk.
    virtual TFuture<void> GetReadyEvent() = 0;

    //! Flushes and closes the writer (both this and the underlying one).
    virtual TFuture<void> Close() = 0;

    //! Returns the reference accounting for all written hunks.
    virtual NTableClient::THunkChunkRef GetHunkChunkRef() const = 0;

    //! Returns the chunk meta.
    virtual NChunkClient::TDeferredChunkMetaPtr GetMeta() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IHunkChunkPayloadWriter)

IHunkChunkPayloadWriterPtr CreateHunkChunkPayloadWriter(
    THunkChunkPayloadWriterConfigPtr config,
    NChunkClient::IChunkWriterPtr underlying,
    int chunkIndex);

////////////////////////////////////////////////////////////////////////////////

NTableClient::IVersionedChunkWriterPtr CreateHunkRefLocalizingVersionedWriterAdapter(
    NTableClient::IVersionedChunkWriterPtr underlying,
    NTableClient::TTableSchemaPtr schema);

////////////////////////////////////////////////////////////////////////////////

//! NB: This call may cause fiber context switch.
NTableClient::TVersionedRow EncodeHunkValues(
    NTableClient::TVersionedRow row,
    const NTableClient::TTableSchema& schema,
    const NTableClient::TRowBufferPtr& rowBuffer,
    const IHunkChunkPayloadWriterPtr& hunkChunkPayloadWriter);

////////////////////////////////////////////////////////////////////////////////

class THunkPayloadReader
{
public:
    THunkPayloadReader(
        NChunkClient::IChunkFragmentReaderPtr chunkFragmentReader,
        NTableClient::TTableSchemaPtr schema);

    TFuture<TSharedRange<NTableClient::TMutableUnversionedRow>> Read(
        TSharedRange<NTableClient::TMutableUnversionedRow> rows,
        NChunkClient::TClientChunkReadOptions options);
    TFuture<TSharedRange<NTableClient::TMutableVersionedRow>> Read(
        TSharedRange<NTableClient::TMutableVersionedRow> rows,
        NChunkClient::TClientChunkReadOptions options);

private:
    template <class TRow>
    class TSessionBase;
    class TUnversionedSession;
    class TVersionedSession;

    const NChunkClient::IChunkFragmentReaderPtr ChunkFragmentReader_;
    const NTableClient::TTableSchemaPtr Schema_;

    template <class TSession, class TRow>
    TFuture<TSharedRange<TRow>> DoRead(
        TSharedRange<TRow> rows,
        NChunkClient::TClientChunkReadOptions options);
};

////////////////////////////////////////////////////////////////////////////////

//! Wraps #underlying with hunk resolution logic.
//! If #schema does not contain hunk columns then #underlying is returned as is.
NTableClient::ISchemafulUnversionedReaderPtr CreateHunkResolvingSchemafulReader(
    NTableClient::ISchemafulUnversionedReaderPtr underlying,
    NChunkClient::IChunkFragmentReaderPtr chunkFragmentReader,
    NTableClient::TTableSchemaPtr schema,
    NChunkClient::TClientChunkReadOptions options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
