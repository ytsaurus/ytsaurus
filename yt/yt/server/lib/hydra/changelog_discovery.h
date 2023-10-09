#pragma once

#include "public.h"

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TChangelogInfo
{
    NElection::TPeerId PeerId =  NElection::InvalidPeerId;
    int ChangelogId = NHydra::InvalidSegmentId;
    int RecordCount = -1;
};

TFuture<std::pair<int, int>> ComputeQuorumLatestChangelogId(
    NHydra::TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    int localChangelogId,
    int localTerm);

//! Looks for a changelog with a given id containing the desired number of records.
/*!
 *  If none are found, then |InvalidSegmentId| is returned in the info.
 */
TFuture<TChangelogInfo> DiscoverChangelog(
    NHydra::TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    int changelogId,
    int minRecordCount);

////////////////////////////////////////////////////////////////////////////////

struct TChangelogQuorumInfo
{
    //! The lower bound for the number of committed records.
    int RecordCountLo;
    //! The upper bound for the number of committed records.
    int RecordCountHi;
};

//! Given #changelogId, computes the quorum info.
TFuture<TChangelogQuorumInfo> ComputeChangelogQuorumInfo(
    NHydra::TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    int changelogId,
    int localRecordCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
