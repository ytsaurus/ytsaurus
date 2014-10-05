#pragma once

#include "public.h"

#include <core/misc/checksum.h>
#include <core/misc/error.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

//! A wrapper around snapshot input stream (either compressed or not).
struct ISnapshotReader
    : public virtual TRefCounted
{
    //! Returns the underlying stream.
    virtual TInputStream* GetStream() = 0;

    //! Returns the snapshot parameters.
    virtual TSnapshotParams GetParams() const = 0;

};

DEFINE_REFCOUNTED_TYPE(ISnapshotReader)

////////////////////////////////////////////////////////////////////////////////

//! A wrapper around snapshot output stream (either compressed or not).
struct ISnapshotWriter
    : public virtual TRefCounted
{
    //! Returns the underlying stream.
    virtual TOutputStream* GetStream() = 0;

    //! Closes the snapshot.
    /*!
     *  The snapshot does not get registered in the store after this call.
     */
    virtual void Close() = 0;

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
    virtual TFuture<TErrorOr<ISnapshotReaderPtr>> CreateReader(int snapshotId) = 0;

    //! Creates a writer for a given snapshot id.
    //! Runs synchronously since it is typically called from a forked child.
    virtual ISnapshotWriterPtr CreateWriter(int snapshotId, const TSharedRef& meta) = 0;

    //! Returns the largest snapshot id not exceeding #maxSnapshotId that is known to exist
    //! in the store or #NonexistingSnapshotId if no such snapshot is present.
    virtual TFuture<TErrorOr<int>> GetLatestSnapshotId(int maxSnapshotId = std::numeric_limits<i32>::max()) = 0;

    //! Confirms an earlier written and closed snapshot.
    /*!
     *  This call actually places the snapshot into the store.
     */
    virtual TFuture<TErrorOr<TSnapshotParams>> ConfirmSnapshot(int snapshotId) = 0;

};

DEFINE_REFCOUNTED_TYPE(ISnapshotStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
