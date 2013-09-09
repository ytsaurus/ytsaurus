#pragma once

#include "public.h"
#include "private.h"
#include "snapshot.h"

#include <core/misc/error.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

//! Manages local snapshots.
/*!
 *  \note Thread affinity: any
 */
class TSnapshotStore
    : public TRefCounted
{
public:
    typedef TErrorOr<TSnapshotReaderPtr> TGetReaderResult;

    //! Creates an instance.
    /*!
     *  \param location Root directory where all snapshot files reside.
     */
    explicit TSnapshotStore(TSnapshotStoreConfigPtr config);

    //! Prepares the snapshot directory.
    void Start();

    //! Gets a reader for a given snapshot id.
    TGetReaderResult GetReader(i32 snapshotId);

    //! Gets a writer for a given snapshot id.
    TSnapshotWriterPtr GetWriter(i32 snapshotId);

    //! Returns the largest snapshot id not exceeding #maxSnapshotId that is known to exist locally
    //! or #NonexistingSnapshotId if no such snapshot is present.
    /*!
     *  \see #OnSnapshotAdded
     */
    i32 GetLatestSnapshotId(i32 maxSnapshotId = std::numeric_limits<i32>::max());

    //! Informs the store that a new snapshot was created.
    void OnSnapshotAdded(i32 snapshotId);

    //! Returns the file name of the snapshot.
    Stroka GetSnapshotFileName(i32 snapshotId);

private:
    TSnapshotStoreConfigPtr Config;
    bool Started;

    TSpinLock SpinLock;
    std::set<i32> SnapshotIds;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
