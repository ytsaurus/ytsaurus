#include "file_node.h"

#include <yt/server/master/cell_master/serialize.h>

namespace NYT::NFileServer {

using namespace NCrypto;

////////////////////////////////////////////////////////////////////////////////

TFileNode* TFileNode::GetTrunkNode()
{
    return TrunkNode_->As<TFileNode>();
}

const TFileNode* TFileNode::GetTrunkNode() const
{
    return TrunkNode_->As<TFileNode>();
}

void TFileNode::Save(NCellMaster::TSaveContext& context) const
{
    TChunkOwnerBase::Save(context);

    using NYT::Save;
    Save(context, MD5Hasher_);
}

void TFileNode::Load(NCellMaster::TLoadContext& context)
{
    TChunkOwnerBase::Load(context);

    using NYT::Load;
    Load(context, MD5Hasher_);
}

void TFileNode::EndUpload(const TEndUploadContext& context)
{
    SetMD5Hasher(context.MD5Hasher);
    TChunkOwnerBase::EndUpload(context);
}

void TFileNode::GetUploadParams(std::optional<TMD5Hasher>* md5Hasher)
{
    *md5Hasher = GetMD5Hasher();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFileServer

