#pragma once

#include "public.h"

#include <core/actions/future.h>

#include <ytlib/election/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TChangelogInfo
{
    TChangelogInfo();

    TPeerId PeerId;
    int ChangelogId;
    int RecordCount;
};

//! Looks for a changelog with a given id containing the desired number of records.
/*!
 *  If none are found, then |NonexistingSegmentId| is returned in the info.
 */
TFuture<TChangelogInfo> DiscoverChangelog(
    TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    int changelogId,
    int minRecordCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
