#pragma once

#include "common.h"
#include "snapshot.h"

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

    //! Initializes an instance.
    /*!
     *  \param location Root directory where all snapshot files reside.
     */
    TSnapshotStore(Stroka location);

    //! Gets a reader for a given snapshot id.
    TSnapshotReader::TPtr GetReader(int snapshotId) const;

    //! Gets a writer for a given snapshot id.
    TSnapshotWriter::TPtr GetWriter(int snapshotId) const;

    //! Gets a raw reader for a given snapshot id.
    TAutoPtr<TFile> GetRawReader(int snapshotId) const;

    //! Gets a writer for a given snapshot id.
    TAutoPtr<TFile> GetRawWriter(int snapshotId) const;

    //! Returns the largest id of the snapshot that is known to exist locally.
    //! or #NonexistingSnapshotId if no snapshots are present.
    /*!
     *  For performance reasons the return value is cached.
     *  
     *  \see #UpdateMaxSnapshotId.
     */
    int GetMaxSnapshotId() const;

    //! Tells the store that a new snapshot was created.
    /*!
     *  This call updated the internally cached value.
     */
    void UpdateMaxSnapshotId(int snapshotId);

private:
    Stroka Location;
    mutable int CachedMaxSnapshotId;

    Stroka GetSnapshotFileName(int snapshotId) const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
