#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/transaction_supervisor/proto/transaction_supervisor.pb.h>

namespace NYT::NTools::NDumpChangelog {

////////////////////////////////////////////////////////////////////////////////

void RegisterProtoMessages()
{
    // If any proto message from the file is referred to, they all become magically registered.
    Y_UNUSED(NYT::NTabletNode::NProto::TReqSplitPartition{});
    Y_UNUSED(NYT::NTransactionSupervisor::NProto::TReqParticipantCommitTransaction{});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTools::NDumpChangelog
