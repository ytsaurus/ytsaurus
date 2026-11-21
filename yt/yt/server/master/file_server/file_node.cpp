#include "file_node.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

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

void TFileNode::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TChunkOwnerBase>();

    PHOENIX_REGISTER_FIELD(1, MD5Hasher_);
}

PHOENIX_DEFINE_TYPE(TFileNode);

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

