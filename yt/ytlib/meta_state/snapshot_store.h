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
    /*!
     *  Reader instances are cached.
     */
    TSnapshotReader::TPtr GetReader(int snapshotId);

    //! Gets a writer for a given snapshot id.
    TSnapshotWriter::TPtr GetWriter(int snapshotId);

    //! Returns the largest id of the snapshot that is known to exist locally.
    //! or #NonexistingSnapshotId if no snapshots are present.
    /*!
     *  For performance reasons the return value is cached.
     *  
     *  \see #UpdateMaxSnapshotId.
     */
    int GetMaxSnapshotId();

    //! Tells the store that a new snapshot was created.
    /*!
     *  This call updated the internally cached value.
     */
    void UpdateMaxSnapshotId(int snapshotId);

private:
    Stroka Location;
    int CachedMaxSnapshotId;

    Stroka GetSnapshotFileName(int snapshotId);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
