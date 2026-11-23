#include "journal_node.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/chunk_server/chunk_list.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NJournalServer {

using namespace NCypressServer;
using namespace NSecurityServer;
using namespace NChunkServer;

////////////////////////////////////////////////////////////////////////////////

void TJournalNode::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TChunkOwnerBase>();

    PHOENIX_REGISTER_FIELD(1, ReadQuorum_);
    PHOENIX_REGISTER_FIELD(2, WriteQuorum_);
    PHOENIX_REGISTER_FIELD(3, Sealed_);
}

PHOENIX_DEFINE_TYPE(TJournalNode);

void TJournalNode::BeginUpload(const TBeginUploadContext& context)
{
    TChunkOwnerBase::BeginUpload(context);

    GetTrunkNode()->Sealed_ = false;
}

TJournalNode* TJournalNode::GetTrunkNode()
{
    return TrunkNode_->As<TJournalNode>();
}

const TJournalNode* TJournalNode::GetTrunkNode() const
{
    return TrunkNode_->As<TJournalNode>();
}

bool TJournalNode::GetSealed() const
{
    return GetTrunkNode()->Sealed_;
}

void TJournalNode::SetSealed(bool value)
{
    YT_VERIFY(IsTrunk());
    Sealed_ = value;
}

TClusterResources TJournalNode::GetDeltaResourceUsage() const
{
    auto* trunkNode = GetTrunkNode();
    if (trunkNode == this) {
        return TBase::GetDeltaResourceUsage();
    } else {
        return trunkNode->GetDeltaResourceUsage(); // Recurse once.
    }
}

TClusterResources TJournalNode::GetTotalResourceUsage() const
{
    auto* trunkNode = GetTrunkNode();
    if (trunkNode == this) {
        return TBase::GetTotalResourceUsage();
    } else {
        return trunkNode->GetTotalResourceUsage(); // Recurse once.
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalServer

