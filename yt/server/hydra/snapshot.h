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
    virtual TInputStream* GetStream() const = 0;

};

////////////////////////////////////////////////////////////////////////////////

//! A wrapper around snapshot output stream (either compressed or not).
struct ISnapshotWriter
    : public virtual TRefCounted
{
    //! Returns the underlying stream.
    virtual TOutputStream* GetStream() const = 0;

    //! Closes the snapshot.
    /*!
     *  The snapshot does not get registered in the store after this call.
     *  #Close is typically called from the forked child process to close the temporary file.
     */
    virtual void Close() = 0;

    //! Confirms an earlier written and closed snapshot.
    /*!
     *  This call actually places the snapshot into the store.
     *  #Confirm is typically called from the parent process to give the temporary file
     *  its final name and update the store.
     */
    virtual void Confirm() = 0;

};

////////////////////////////////////////////////////////////////////////////////

//! Parameters used for creating new snapshots.
struct TSnapshotCreateParams
{
    TSnapshotCreateParams();

    int PrevRecordCount;
};

//! Parameters of an existing snapshot.
struct TSnapshotParams
{
    TSnapshotParams();

    int PrevRecordCount;
    TChecksum Checksum;
    i64 CompressedLength;
    i64 UncompressedLength;

};

//! Manages a collection snapshots.
struct ISnapshotStore
    : public virtual TRefCounted
{
    virtual const TCellGuid& GetCellGuid() const = 0;

    //! Returns parameters of a given snapshot.
    //! Returns |Null| if the snapshot is not found.
    virtual TNullable<TSnapshotParams> TryGetSnapshotParams(int snapshotId) = 0;

    //! Creates a reader for a given snapshot id.
    //! Returns |nullptr| is the snapshot is not found.
    virtual ISnapshotReaderPtr TryCreateReader(int snapshotId) = 0;

    //! Creates a raw reader for a given snapshot id.
    //! The latter reader accesses the compressed snapshot image and
    //! can be useful for, e.g., distributing snapshots among peers.
    //! Throws on failure.
    virtual ISnapshotReaderPtr TryCreateRawReader(
        int snapshotId,
        i64 offset) = 0;

    //! Creates a writer for a given snapshot id.
    virtual ISnapshotWriterPtr CreateWriter(
        int snapshotId,
        const TSnapshotCreateParams& params) = 0;

    //! Creates a writer for a given snapshot id.
    //! Like #CreateRawReader, this method provides means to write the compressed snapshot image.
    virtual ISnapshotWriterPtr CreateRawWriter(int snapshotId) = 0;

    //! Returns the largest snapshot id not exceeding #maxSnapshotId that is known to exist
    //! in the store or #NonexistingSnapshotId if no such snapshot is present.
    virtual int GetLatestSnapshotId(int maxSnapshotId = std::numeric_limits<i32>::max()) = 0;


    // Extension methods.

    //! Returns parameters of a given snapshot.
    //! Throws if the snapshot is not found.
    TSnapshotParams GetSnapshotParamsOrThrow(int snapshotId);

    //! Creates a reader for a given snapshot id.
    //! Throws on failure.
    ISnapshotReaderPtr CreateReaderOrThrow(int snapshotId);

    //! Creates a reader for a given snapshot id.
    //! Throws on failure.
    ISnapshotReaderPtr CreateRawReaderOrThrow(
        int snapshotId,
        i64 offset);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
