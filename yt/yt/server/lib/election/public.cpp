#include "public.h"

#include <yt/yt/ytlib/election/proto/election_service.pb.h>

namespace NYT::NElection::NProto {

////////////////////////////////////////////////////////////////////////////////

void ToProto(TPeerPriority* protoPeerPriority, const NElection::TPeerPriority& peerPriority)
{
    protoPeerPriority->set_first(peerPriority.first);
    protoPeerPriority->set_second(peerPriority.second);
}

void FromProto(NElection::TPeerPriority* peerPriority, const TPeerPriority& protoPeerPriority)
{
    peerPriority->first = protoPeerPriority.first();
    peerPriority->second = protoPeerPriority.second();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection

