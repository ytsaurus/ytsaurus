#pragma once

#include "public.h"

#include <yt/yt/ytlib/hydra/proto/hydra_manager.pb.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/async_stream.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

//! A wrapper around snapshot input stream (either compressed or raw).
struct ISnapshotReader
    : public NConcurrency::IAsyncZeroCopyInputStream
{
    //! Opens the reader.
    virtual TFuture<void> Open() = 0;

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
    virtual TFuture<void> Open() = 0;

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
    NProto::TSnapshotMeta Meta;
    TChecksum Checksum = 0;
    i64 CompressedLength = -1;
    i64 UncompressedLength = -1;
};

struct TRemoteSnapshotParams
    : public TSnapshotParams
{
    TPeerId PeerId = InvalidPeerId;
    int SnapshotId = InvalidSegmentId;
    bool SnapshotReadOnly = false;
};

//! Manages a collection of snapshots on a peer.
struct ISnapshotStore
    : public virtual TRefCounted
{
    //! Creates a reader for a given snapshot id.
    /*!
     *  The reader must be opened before usage.
     *
     *  Attempting to read a non-existent snapshot may result in:
     *    - CreateReader throwing a EErrorCode::NoSuchSnapshot exception;
     *    - CreateReader returning without throwing but subsequent opening of
     *      the reader throwing a EErrorCode::NoSuchSnapshot exception;
     *    - both CreateReader and opening the reader returning without throwing
     *      but the latter resulting in an (async) EErrorCode::NoSuchSnapshot error.
     *  The client must be prepared to handle all of these scenarios.
     */
    virtual ISnapshotReaderPtr CreateReader(int snapshotId) = 0;

    //! Creates a writer for a given snapshot id.
    /*!
     *  The writer must be opened before usage.
     *  Once the writer is closed the snapshot appears visible in the store.
     */
    virtual ISnapshotWriterPtr CreateWriter(int snapshotId, const NProto::TSnapshotMeta& meta) = 0;

    //! Returns the largest snapshot id not exceeding #maxSnapshotId that is known to exist
    //! in the store or #InvalidSegmentId if no such snapshot is present.
    virtual TFuture<int> GetLatestSnapshotId(int maxSnapshotId = std::numeric_limits<i32>::max()) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISnapshotStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
