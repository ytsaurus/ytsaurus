#include "journal_node.h"

#include <yt/server/master/cell_master/serialize.h>

#include <yt/server/master/chunk_server/chunk_list.h>

namespace NYT::NJournalServer {

using namespace NCypressServer;
using namespace NSecurityServer;
using namespace NChunkServer;

////////////////////////////////////////////////////////////////////////////////

void TJournalNode::Save(NCellMaster::TSaveContext& context) const
{
    TChunkOwnerBase::Save(context);

    using NYT::Save;
    Save(context, ReadQuorum_);
    Save(context, WriteQuorum_);
    Save(context, Sealed_);
}

void TJournalNode::Load(NCellMaster::TLoadContext& context)
{
    TChunkOwnerBase::Load(context);

    using NYT::Load;
    Load(context, ReadQuorum_);
    Load(context, WriteQuorum_);
    Load(context, Sealed_);
}

void TJournalNode::BeginUpload(const TBeginUploadContext& context)
{
    TChunkOwnerBase::BeginUpload(context);

    GetTrunkNode()->Sealed_ = false;
}

TChunk* TJournalNode::GetTrailingChunk() const
{
    if (!ChunkList_) {
        return nullptr;
    }

    if (ChunkList_->Children().empty()) {
        return nullptr;
    }

    return ChunkList_->Children().back()->AsChunk();
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

