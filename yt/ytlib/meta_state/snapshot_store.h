#pragma once

#include "public.h"
#include "meta_state_manager_proxy.h"

#include <ytlib/misc/error.h>

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
    typedef TMetaStateManagerProxy::EErrorCode EErrorCode;
    typedef TValueOrError<TSnapshotReaderPtr> TGetReaderResult;

    //! Creates an instance.
    /*!
     *  \param location Root directory where all snapshot files reside.
     */
    TSnapshotStore(TSnapshotStoreConfigPtr config);

    //! Prepares the snapshot directory.
    void Start();

    //! Gets a reader for a given snapshot id.
    TGetReaderResult GetReader(i32 snapshotId) const;

    //! Gets a writer for a given snapshot id.
    TSnapshotWriterPtr GetWriter(i32 snapshotId) const;

    //! Returns the largest snapshot id not exceeding #maxSnapshotId that is known to exist locally
    //! or #NonexistingSnapshotId if no such snapshot is present.
    /*!
     *  \see #OnSnapshotAdded
     */
    i32 LookupLatestSnapshot(i32 maxSnapshotId = std::numeric_limits<i32>::max());

    //! Informs the store that a new snapshot was created.
    void OnSnapshotAdded(i32 snapshotId);

    //! Returns the file name of the snapshot.
    Stroka GetSnapshotFileName(i32 snapshotId) const;

private:
    TSnapshotStoreConfigPtr Config;
    bool Started;

    TSpinLock SpinLock;
    std::set<i32> SnapshotIds;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
