#pragma once

#include "common.h"
#include "snapshot.h"
#include "meta_state_manager_proxy.h"

#include "../misc/error.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

//! Manages local snapshots.
/*!
 *  Thread affinity: single-threaded.
 */
class TSnapshotStore
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TSnapshotStore> TPtr;
    typedef TMetaStateManagerProxy::EErrorCode EErrorCode;

    //! Initializes an instance.
    /*!
     *  \param location Root directory where all snapshot files reside.
     */
    TSnapshotStore(const Stroka& path);

    typedef TValueOrError<TSnapshotReader::TPtr> TGetReaderResult;
    //! Gets a reader for a given snapshot id.
    TGetReaderResult GetReader(int snapshotId) const;

    //! Gets a writer for a given snapshot id.
    TSnapshotWriter::TPtr GetWriter(int snapshotId) const;

    typedef TValueOrError< TSharedPtr<TFile> > TGetRawReaderResult;
    //! Gets a raw reader for a given snapshot id.
    TGetRawReaderResult GetRawReader(int snapshotId) const;

    //! Gets a writer for a given snapshot id.
    TSharedPtr<TFile> GetRawWriter(int snapshotId) const;

    //! Returns the largest id of the snapshot that is known to exist locally.
    //! or #NonexistingSnapshotId if no snapshots are present.
    /*!
     *  For performance reasons the return value is cached.
     *  
     *  \see #UpdateMaxSnapshotId
     */
    int GetMaxSnapshotId() const;

    //! Tells the store that a new snapshot was created.
    /*!
     *  This call updated the internally cached value.
     */
    void UpdateMaxSnapshotId(int snapshotId);

private:
    Stroka Path;
    mutable int CachedMaxSnapshotId;

    Stroka GetSnapshotFileName(int snapshotId) const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
