#pragma once

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TPeerPriority;

void ToProto(TPeerPriority* protoPeerPriority, const NElection::TPeerPriority& peerPriority);
void FromProto(NElection::TPeerPriority* peerPriority, const TPeerPriority& protoPeerPriority);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

using TPeerIdSet = THashSet<int>;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IElectionCallbacks)
DECLARE_REFCOUNTED_STRUCT(IElectionManager)

DECLARE_REFCOUNTED_STRUCT(TEpochContext)

DECLARE_REFCOUNTED_CLASS(TDistributedElectionManager)
DECLARE_REFCOUNTED_CLASS(TElectionManagerThunk)

DECLARE_REFCOUNTED_CLASS(TDistributedElectionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection
