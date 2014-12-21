#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/concurrency/async_stream.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

//! A wrapper around snapshot input stream (either compressed or raw).
struct ISnapshotReader
    : public NConcurrency::IAsyncInputStream
{
    //! Opens the reader.
    virtual TAsyncError Open() = 0;

    //! Returns the snapshot parameters.
    virtual TSnapshotParams GetParams() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISnapshotReader)

////////////////////////////////////////////////////////////////////////////////

//! A wrapper around snapshot output stream (either compressed or raw).
struct ISnapshotWriter
    : public NConcurrency::IAsyncOutputStream
{
    //! Opens the writer.
    virtual TAsyncError Open() = 0;

    //! Closes and registers the snapshot.
    virtual TAsyncError Close() = 0;

    //! Returns the snapshot parameters.
    /*
     *  Can only be called after the writer is closed.
     */
    virtual TSnapshotParams GetParams() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISnapshotWriter)

////////////////////////////////////////////////////////////////////////////////

//! Parameters of an existing snapshot.
struct TSnapshotParams
{
    TSharedRef Meta;
    TChecksum Checksum = 0;
    i64 CompressedLength = -1;
    i64 UncompressedLength = -1;
};

//! Manages a collection snapshots.
struct ISnapshotStore
    : public virtual TRefCounted
{
    //! Creates a reader for a given snapshot id.
    virtual ISnapshotReaderPtr CreateReader(int snapshotId) = 0;

    //! Creates a writer for a given snapshot id.
    /*!
     *  The writer must be opened before usage.
     *  Once the writer is closed the snapshot appears visible in the store.
     */
    virtual ISnapshotWriterPtr CreateWriter(int snapshotId, const TSharedRef& meta) = 0;

    //! Returns the largest snapshot id not exceeding #maxSnapshotId that is known to exist
    //! in the store or #NonexistingSnapshotId if no such snapshot is present.
    virtual TFuture<TErrorOr<int>> GetLatestSnapshotId(int maxSnapshotId = std::numeric_limits<i32>::max()) = 0;

};

DEFINE_REFCOUNTED_TYPE(ISnapshotStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
